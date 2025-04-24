use std::{env, error::Error, fs::{self, File}, io::{BufReader, Read, Seek, SeekFrom, Write}, path::{Path, PathBuf}, process::exit, str::FromStr, sync::{atomic::{AtomicBool, Ordering}, Arc}, u64};

use flume::Receiver;
use gzp::{check::{Adler32, Check}, deflate::Zlib, par::compress::{ParCompress, ParCompressBuilder}, FormatSpec};
use indicatif::{MultiProgress, ProgressBar};
use serde::{Deserialize, Serialize};

const EXT: &str = "zlib";
const EXT_PARTIAL: &str = "comp_dat";

fn main() {

    unsafe { env::set_var("RUST_LOG", "debug") };
    env_logger::init();

    let cancelled = Arc::new(AtomicBool::new(false));
    let c = cancelled.clone();

    ctrlc::set_handler(move || {
        c.store(true, Ordering::Relaxed)
    }).expect("Error setting Ctrl-C handler!");

    let args = std::env::args().collect::<Vec<_>>();

    if args.len() <= 1 {
        println!("Utility for compressing files, this utility makes a new file of the same name except with the file extension; `.zlib`.");
        println!("Usage: {} <filename> <filename> ...", args[0]);
        exit(1);
    }

    let progress_bar = MultiProgress::new();
    let total_progress = ProgressBar::new(args.len() as u64 - 1);
    progress_bar.add(total_progress.clone());

    // This is the buffer the data gets streamed into.
    let mut data_buf = [0u8; 2048];

    for file_name in &args[1..] {

        let compress_file = match FileToCompress::from_filename(file_name) {
            Ok(v) => v,
            Err(_) => continue,
        };

        let mut src_file = match compress_file.get_src_file_and_seek() {
            Ok(v) => v,
            Err(_) => {
                eprintln!("Failed to find file: {:?}", file_name);
                continue;
            },
        };

        let file_length = src_file.get_ref().metadata().map(|v| v.len()).unwrap_or(u64::MAX);

        let file_progress = ProgressBar::new(file_length);
        progress_bar.add(file_progress.clone());
        let (mut compressor, checksum_rx) = compress_file.to_compressor::<Zlib>().unwrap();

        let mut total_byte_count = compress_file.get_starting_src_file_byte();

        total_progress.set_position(total_byte_count);

        while let Ok(byte_count) = src_file.read(&mut data_buf) {
            if byte_count == 0 { break; }
            let passed_data = &data_buf[0..byte_count];
            compressor.write_all(passed_data).unwrap();

            total_byte_count += byte_count as u64;
            file_progress.set_position(total_byte_count);

            if cancelled.load(Ordering::Relaxed) {
                log::info!("Stopping compression because of CTRL+C! Stopped on file: {}", compress_file.src_filename.to_string_lossy());
                break;
            }

        }

        file_progress.finish_and_clear();
        progress_bar.remove(&file_progress);
        total_progress.inc(1);

        if !cancelled.load(Ordering::Relaxed) {
            // Cleanup if we didn't cancel (we finished successfully).
            if let Ok(_) = fs::remove_file(&compress_file.stp_filename) {
                log::info!("Removed file: {}", compress_file.stp_filename.to_string_lossy());
            }

            compressor.set_write_footer_on_exit(true);
            drop(compressor);

            log::info!("Successfully Compressed file: {} -> {}",
                compress_file.src_filename.to_string_lossy(),
                compress_file.dst_filename.to_string_lossy(),
                );

        } else {

            let dict = compressor.get_dict().map(|v| v.to_vec());
            drop(compressor);
            let checksum = match checksum_rx.recv() {
                Ok(v) => v,
                Err(_) => panic!("Failed to get checksum for partially compressed file: {}!", compress_file.dst_filename.to_string_lossy()),
            };

            while let Ok(v) = checksum_rx.recv() {
                log::error!("Here is an extra checksum value: {}",
                    serde_json::to_string(&v).unwrap_or("FAILED_TO_SERIALIZE_CHECKSUM!".into())
                    );
            }

            // Ensures the counts are correct.
            assert_eq!(checksum.amount() as u64, total_byte_count);

            let compressor_state = CompressorState::new(checksum, dict, total_byte_count);
            log::info!("Writing Compressor State: `{}` to disk.", compressor_state);
            if let Err(e) = compressor_state.to_file(&compress_file.stp_filename) {
                panic!("Error writing to compressed state file: {}! Error: {:?}.", compress_file.stp_filename.to_string_lossy(), e);
            }

        }

    }

    total_progress.finish();

}

struct FileToCompress {
    src_filename: PathBuf,
    dst_filename: PathBuf,
    stp_filename: PathBuf,
}

impl FileToCompress {

    pub fn from_filename(filename: &str) -> Result<Self, Box<dyn Error>> {

        let src_filename = PathBuf::from_str(filename)?;
        let dst_filename = Self::extend_extension(&src_filename, EXT);
        let stp_filename = Self::make_hidden_filename(&Self::extend_extension(&dst_filename, EXT_PARTIAL));

        return Ok(Self {
            src_filename,
            dst_filename,
            stp_filename,
        });

    }

    /// Gets the location in the source file to continue from.
    pub fn get_starting_src_file_byte(&self) -> u64 {
        return self.get_compressor_state().map(|v| v.total_byte_count).unwrap_or(0);
    }

    /// Adds an extention to the [`PathBuf`].
    /// Ex: foo.txt, rs => foo.txt.rs.
    ///     foo    , rs => foo.rs.
    pub(crate) fn extend_extension(src: &PathBuf, ext: &str) -> PathBuf {
        return src.with_extension(src.extension().map(|v| v.to_string_lossy().to_string() + ".").unwrap_or(String::new()) + ext);
    }

    /// Adds an extention to the [`PathBuf`].
    /// Ex: foo.txt => .foo.txt
    ///     foo     => .foo
    pub(crate) fn make_hidden_filename(src: &PathBuf) -> PathBuf {
        let mut out_src = src.clone();
        out_src.set_file_name(format!(".{}", src.file_name().unwrap_or_default().to_string_lossy()));
        return out_src;
    }

    /// Opens source file.
    pub fn get_src_file(&self) -> std::io::Result<BufReader<File>> {
        return Ok(BufReader::new(File::open(&self.src_filename)?));
    }

    /// Opens source file to the correct section and returns. Opens the compressor state file in order
    /// to do so.
    pub fn get_src_file_and_seek(&self) -> std::io::Result<BufReader<File>> {

        let mut file = self.get_src_file()?;

        if let Err(e) = file.seek(SeekFrom::Start(self.get_starting_src_file_byte())) {
            log::warn!("Failed to seek to area on file (for whatever reason). Error: {:?}.", e);
        }

        return Ok(file);
    }

    /// Opens the destination file to the correct section and returns. Checks to see if the stop
    /// file exists, if it does but we need to create a new file, we remove the stop file. If we
    /// don't need to create a new file but no stop file exists, we remove the existing file.
    pub fn get_dst_file_and_seek(&self) -> std::io::Result<File> {

        if self.stp_filename.is_file() {

            if !self.dst_filename.is_file() {
                let _ = fs::remove_file(&self.stp_filename);
                return File::create(&self.dst_filename);
            }

            else {
                return File::options()
                    .append(true)
                    .open(&self.dst_filename)
                    ;
            }

        }

        if self.dst_filename.is_file() {
            let _ = fs::remove_file(&self.stp_filename);
        }

        return File::create(&self.dst_filename);

    }

    /// Tries to get the compressor state from file.
    pub fn get_compressor_state(&self) -> Result<CompressorState, Box<dyn Error>> {
        let state = CompressorState::from_file(&self.stp_filename)?;
        return Ok(state);
    }

    /// Creates a compressor based on the files that the struct is aware of.
    pub fn to_compressor<T>(&self) -> Result<(ParCompress<T>, Receiver<<T as FormatSpec>::C>), Box<dyn Error>>
        where
            T: FormatSpec,
            <T as FormatSpec>::C: From<Adler32>,
    {

        let (cs_tx, cs_rx) = ParCompressBuilder::<T>::checksum_channel();

        // This is very unlikely to error.
        let out_file = self.get_dst_file_and_seek()?;

        let mut builder = ParCompressBuilder::new()
            .checksum_dest(Some(cs_tx))
            .write_header_on_start(!self.stp_filename.is_file())
            .write_footer_on_exit(false)
            ;

        if let Ok(compressor_config) = CompressorState::from_file(&self.stp_filename) {

            log::info!("Creating compressor with config: {}.", compressor_config);

            // builder = builder.dictionary(compressor_config.dictionary.map(|v| v.into()));

            // Gets checksum
            let mut deserializer = serde_json::Deserializer::from_str(&compressor_config.checksum_serialized);
            let check = Adler32::to_deserialized(&mut deserializer)?;

            builder = builder.checksum(Some(check.into()));

        }

        let par_zlib: ParCompress<T> = builder
            .from_writer(out_file)
            ;

        return Ok((par_zlib, cs_rx));

    }

}

#[derive(Serialize, Deserialize, Default, Debug)]
pub struct CompressorState {
    pub checksum_serialized: String,
    pub dictionary: Option<Vec<u8>>,
    pub total_byte_count: u64,
}

impl CompressorState {

    /// Creates a new instance of the [`CompressorState`].
    pub fn new(checksum: impl Check + Send, dictionary: Option<Vec<u8>>, total_byte_count: u64) -> Self {

        let mut serializer = serde_json::Serializer::new(Vec::new());
        checksum.to_serialized(&mut serializer).unwrap();
        let checksum_serialized = String::from_utf8_lossy(&serializer.into_inner()).to_string();

        return Self {
            checksum_serialized,
            dictionary,
            total_byte_count,
        };
    }

    /// Reads state from file.
    pub fn from_file(path: &Path) -> Result<Self, Box<dyn Error>> {

        let mut file = File::open(path)?;
        let mut out_string = String::new();
        file.read_to_string(&mut out_string)?;

        return Ok(serde_json::from_str::<Self>(&out_string)?);

    }

    /// Writes the state to file.
    pub fn to_file(&self, path: &Path) -> Result<(), Box<dyn Error>> {

        let data = serde_json::to_string(&self)?;

        if let Ok(_) = fs::remove_file(&path) {
            log::info!("Removed previous compressor state file: {:?}", path);
        }

        let mut file = File::create_new(&path)?;
        file.write_all(data.as_bytes())?;

        return Ok(());

    }

}

impl std::fmt::Display for CompressorState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {

        let bytes_written = self.total_byte_count;
        let checksum = self.checksum_serialized.clone();

        let dictionary = match self.dictionary.is_some() {
            true => "Set",
            false => "Not Set",
        }.to_string();

        return write!(f, "CompressorState: {{ Bytes Written: {bytes_written} Bytes, Checksum: {checksum}, Dictionary: ({dictionary}) }}");
    }
}
