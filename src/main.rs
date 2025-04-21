use std::{env, error::Error, fs::{self, File}, io::{Read, Seek, SeekFrom, Write}, os::unix::fs::FileExt, path::{Path, PathBuf}, process::exit, str::FromStr, sync::{atomic::{AtomicBool, Ordering}, Arc}, u64};

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

        let src_file = match compress_file.get_src_file_and_seek() {
            Ok(v) => v,
            Err(_) => {
                eprintln!("Failed to find file: {:?}", file_name);
                continue;
            },
        };

        let file_length = src_file.metadata().map(|v| v.len()).unwrap_or(u64::MAX);

        let file_progress = ProgressBar::new(file_length);
        progress_bar.add(file_progress.clone());
        let _ = compress_file.check_files();
        let (mut compressor, checksum_rx) = compress_file.to_compressor::<Zlib>().unwrap();

        let mut total_byte_count = 0u64;

        while let Ok(byte_count) = src_file.read_at(&mut data_buf, total_byte_count) {
            if byte_count == 0 { break; }
            let passed_data = &data_buf[0..byte_count];
            compressor.write_all(passed_data).unwrap();
            file_progress.inc(byte_count as u64);

            total_byte_count += byte_count as u64;

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

            let compressor_state = CompressorState::new(checksum, dict, total_byte_count);
            log::info!("Writing Compressor State: `{:?}` to disk.", compressor_state);
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
        let stp_filename = Self::extend_extension(&dst_filename, EXT_PARTIAL);

        return Ok(Self {
            src_filename,
            dst_filename,
            stp_filename,
        });

    }

    /// Adds an extention to the [`PathBuf`].
    /// Ex: foo.txt, rs => foo.txt.rs.
    ///     foo    , rs => foo.rs.
    pub(crate) fn extend_extension(src: &PathBuf, ext: &str) -> PathBuf {
        return src.with_extension(src.extension().map(|v| v.to_string_lossy().to_string() + ".").unwrap_or(String::new()) + ext);
    }

    /// Opens source file.
    pub fn get_src_file(&self) -> std::io::Result<File> {
        return File::open(&self.src_filename);
    }

    /// Opens source file to correct section and returns. Opens the compressor state file in order
    /// to do so.
    pub fn get_src_file_and_seek(&self) -> std::io::Result<File> {

        let seek_to = self.get_compressor_state().map(|v| v.total_byte_count).unwrap_or(0);
        let mut file = self.get_src_file()?;

        if let Err(e) = file.seek(SeekFrom::Start(seek_to)) {
            log::warn!("Failed to seek to area on file (for whatever reason). Error: {:?}.", e);
        }

        return Ok(file);

    }

    /// Tries to get the compressor state from file.
    pub fn get_compressor_state(&self) -> Result<CompressorState, Box<dyn Error>> {
        let state = CompressorState::from_file(&self.stp_filename)?;
        return Ok(state);
    }

    pub fn check_files(&self) -> bool {

        if !self.src_filename.is_file() {
            return false;
        }

        // If the destination already exists
        if self.dst_filename.is_file() {
            // But the destination partial file doesn't
            if !self.stp_filename.is_file() {
                // It isn't good
                return false;
            }
        }

        if self.stp_filename.is_file() && !self.dst_filename.is_file() {
            log::warn!("The partial compression file exists but the destination file itself doesn't! File: {}",
                self.src_filename.to_string_lossy());
        }

        return true;

    }

    /// Creates a compressor based on the files that the struct is aware of.
    pub fn to_compressor<T>(&self) -> Result<(ParCompress<T>, Receiver<<T as FormatSpec>::C>), Box<dyn Error>>
        where
            T: FormatSpec,
            <T as FormatSpec>::C: From<Adler32>,
    {

        let (cs_tx, cs_rx) = ParCompressBuilder::<T>::checksum_channel();

        let output_exists = self.stp_filename.is_file();

        let out_file = match &self.dst_filename.is_file() {
            true => {
                if !output_exists {
                    let _ = fs::remove_file(&self.dst_filename);
                    File::create_new(&self.dst_filename)?
                }
                else {
                    let mut file = File::options().write(true).open(&self.dst_filename)?;
                    // Seeks to the end to ensure we keep writing to the correct spot.
                    file.seek(SeekFrom::End(0))?;
                    file
                }
            },
            false => {
                File::create_new(&self.dst_filename)?
            },
        };

        let mut builder = ParCompressBuilder::new()
            .checksum_dest(Some(cs_tx))
            .write_header_on_start(!output_exists)
            .write_footer_on_exit(false)
            ;

        if output_exists {

            let compressor_config = CompressorState::from_file(&self.stp_filename).unwrap();
            builder = builder.dictionary(compressor_config.dictionary.map(|v| v.into()));
            let check = Adler32 {
                sum: compressor_config.check_sum,
                amount: compressor_config.check_amount,
            };

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
    pub check_sum: u32,
    pub check_amount: u32,
    pub dictionary: Option<Vec<u8>>,
    pub total_byte_count: u64,
}

impl CompressorState {

    /// Creates a new instance of the [`CompressorState`].
    pub fn new(checksum: impl Check + Send, dictionary: Option<Vec<u8>>, total_byte_count: u64) -> Self {

        let check_sum = checksum.sum();
        let check_amount = checksum.amount();

        return Self {
            check_sum,
            check_amount,
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
