# Cancellable Compression
This command line utility was created as a proof of concept implementation for the serialization of compression
state. What does that mean? You can cancel the progress of this compression utility at any time
using `ctrl+c` and re-run the compression on the same file and continue compression from where
you left off, even if you reboot your system or the power goes out (though this isn't something that is handled).
This can be helpful for large files where compression times may be lengthy.

Usage:
```
# With one file
cancellable-compression <FILE>
# With multiple files, you can use an arbitrary amount of files.
cancellable-compression <FILE1> <FILE2>
```

The name of the compressed files looks like: `file.txt`  > `file.txt.zlib`.

While multiple files can be specified, this doesn't mean files get processed in parallel. If parallel file
compression is needed, this tool with something like GNU parallel would probably be the way to go.

---

This utility was built using (and for) [gzp](https://github.com/sstadick/gzp/) - a rust library for parallel compression.
