# Key-Value-File-System
Key/value file system that utilizes raw and direct I/O operations on a block device. Avoids reliance on kernel buffers for both reading and writing by incorporating efficient write buffering and read caching mechanisms to optimize performance

How to run:
dd if=/dev/zero of=output.db bs=4096 count=1024 
make
./cs238p output.db
