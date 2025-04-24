#!/usr/bin/sh

cargo build --release
./target/release/cancellable-compression tests/out.png
echo "Decompressing..."
zlib-flate -uncompress < tests/out.png.zlib > tests/decomp_img.png
echo "Comparing Checksums..."
sha256sum tests/out.png
sha256sum tests/decomp_img.png
pngcheck -cp7t tests/decomp_img.png
rm tests/*.zlib
rm tests/decomp_img.png
