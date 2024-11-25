/**
 * Tony Givargis
 * Copyright (C), 2023
 * University of California, Irvine
 *
 * CS 238P - Operating Systems
 * logfs.h
 */

#ifndef _LOGFS_H_
#define _LOGFS_H_

#include "system.h"

struct logfs;

/**
 * Opens the block device specified in pathname for buffered I/O using an
 * append only log structure.
 * It should create buffers for read and write, based on block size of device and the number of buffers mentioned in the file
 * pathname: the pathname of the block device
 *
 * return: an opaque handle or NULL on error
 */

struct logfs *logfs_open(const char *pathname);

/**
 * Closes a previously opened logfs handle.
 *
 * logfs: an opaque handle previously obtained by calling logfs_open()
 *
 * Note: logfs may be NULL.
 */

void logfs_close(struct logfs *logfs);

/**
 * Random read of len bytes at location specified in off from the logfs.
 * device_read only works in terms of block sizes
 * This function should first calulate block aligned offset and length and use device_read to copy it to read buffer in logfs and then copy 
 * the relavant len number of bytes to buff
 * logfs: an opaque handle previously obtained by calling logfs_open()
 * buf  : a region of memory large enough to receive len bytes
 * off  : the starting byte offset
 * len  : the number of bytes to read
 *
 * return: 0 on success, otherwise error
 */

int logfs_read(struct logfs *logfs, void *buf, uint64_t off, size_t len);

/**
 * Append len bytes to the logfs.
 * It should copy the buf to write buffer in logfs and then use logfs_flush to block sized bytes to device
 * the write buffer is implemented like a circular queue, with all the details like head, tail size and relavant information stored in logfs.
 * This function will append bytes to the head if the queue is not full
 * 
 * logfs: an opaque handle previously obtained by calling logfs_open()
 * buf  : a region of memory holding the len bytes to be written
 * len  : the number of bytes to write
 *
 * return: 0 on success, otherwise error
 */

int logfs_append(struct logfs *logfs, const void *buf, uint64_t len);



/**
 * It will append a block sized byte using device_write.
 * logfs_read will add to queue and logfs_flush will remove from queue. 
 * since device_write only writes interms of block sizes, logfs should flush only when the size of queue is greater than the block size.
 *
 * logfs: an opaque handle previously obtained by calling logfs_open()
 * 
 *
 * return: 0 on success, otherwise error
 */

int logfs_flush(struct logfs *logfs);

#endif /* _LOGFS_H_ */
