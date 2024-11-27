/**
 * Tony Givargis
 * Copyright (C), 2023
 * University of California, Irvine
 *
 * CS 238P - Operating Systems
 * logfs.c
 */

#include <pthread.h>
#include <stdlib.h>
#include "device.h"
#include "logfs.h"

#define WCACHE_BLOCKS 32
#define RCACHE_BLOCKS 256

struct logfs {
    struct device *device;
    pthread_t worker_thread;
    pthread_mutex_t queue_mutex;
    pthread_cond_t queue_not_empty;
    void *write_queue;
    uint64_t write_head;
    uint64_t write_tail;
    uint64_t write_limit;
    void *read_queue;
    uint64_t read_start_addr;
    uint64_t read_end_addr;
    uint64_t block_size;


    uint64_t device_limit;

    uint64_t done;
    /*
    Thread Specific Variables
    */
    pthread_t thread;
    uint64_t write_allow;
    uint64_t read_allow;
    uint64_t should_exit;
    pthread_mutex_t mutex;
    pthread_mutex_t print;
    pthread_cond_t can_write;
    pthread_cond_t can_read;
};

void *writer_function(void * arg);
void *reader_function(struct logfs *fs);


struct logfs *logfs_open(const char *pathname) {
    struct logfs *fs;

    assert(pathname);

    if (!(fs = malloc(sizeof(struct logfs)))) {
        TRACE("out of memory");
        return NULL;
    }

    fs->device = device_open(pathname);
    if (!fs->device) {
        TRACE(0);
        FREE(fs);
        return NULL;
    }
    fs->block_size = device_block(fs->device);

    if (!(fs->write_queue = aligned_alloc(fs->block_size,fs->block_size * WCACHE_BLOCKS))) {
        TRACE("out of memory");
        return NULL;
    }
    memset(fs->write_queue, 0, fs->block_size * WCACHE_BLOCKS);
    if (!(fs->read_queue = aligned_alloc(fs->block_size,fs->block_size * RCACHE_BLOCKS))) {
        TRACE("out of memory");
        return NULL;
    }
    memset(fs->read_queue, 0, fs->block_size * RCACHE_BLOCKS);
    fs->read_start_addr = 0;
    fs->read_end_addr = 0;
    fs->write_limit = (uint64_t)fs->write_queue + (fs->block_size * WCACHE_BLOCKS);
    fs->write_head = (uint64_t)fs->write_queue;
    fs->write_tail = (uint64_t)fs->write_queue;
    fs->device_limit = 0;
    
    /*
    Thread specific variables
    */
    fs->should_exit = 0;
    fs->write_allow = 0;
    fs->read_allow = 0;
    fs->done = 0;

    if (pthread_mutex_init(&(fs->mutex), NULL) != 0) {

        perror("Mutex initialization failed");
    }
    if (pthread_mutex_init(&(fs->print), NULL) != 0) {

        perror("Mutex initialization failed");
    }
    if (pthread_cond_init(&(fs->can_write), NULL) != 0) {
        perror("Condition variable 1 initialization failed");
    }
    if (pthread_cond_init(&(fs->can_read), NULL) != 0) {
        perror("Condition variable 2 initialization failed");
    }
    pthread_create(&fs->thread, NULL, writer_function, (void *)fs);
    return fs;
}

void logfs_close(struct logfs *fs) {
    if (fs) {
        fs->should_exit = 1;
        fs->write_allow = 1;
        pthread_cond_signal(&fs->can_write);
        pthread_join(fs->thread, NULL);
        pthread_mutex_destroy(&fs->mutex);
        pthread_cond_destroy(&fs->can_write);
        pthread_cond_destroy(&fs->can_read);
        FREE(fs->read_queue);
        fs->read_queue = NULL;
        FREE(fs->write_queue);
        fs->write_queue = NULL;
        device_close(fs->device);
        fs->device = NULL;
        FREE(fs);
    }
}


int logfs_read(struct logfs *fs, void *buf, uint64_t off, uint64_t len) {
    uint64_t file_size;
    uint64_t read_addr;
    uint64_t diff;
    uint64_t read_from_align;
    uint64_t new_len;
    uint64_t len_diff;
    uint64_t len_align;

    if((off + len > fs->read_end_addr) || off < fs->read_start_addr){
        reader_function(fs);
        diff = off % fs->block_size;
        read_from_align = off - diff;
        new_len = len+diff;
        len_diff = new_len % fs->block_size;
        if(len_diff > 0){
            new_len = new_len - len_diff + fs->block_size;
        }
        device_read(fs->device, fs->read_queue, read_from_align, new_len);
        fs->read_start_addr = read_from_align;
        fs->read_end_addr = read_from_align + new_len;
        if(fs->read_end_addr > fs->device_limit)
        {
            fs->read_end_addr = fs->device_limit;
        }
    }

    file_size = device_size(fs->device);
    if (len + off > file_size) {
        TRACE("The given read length exceeds device size");
        return -1;
    }

    if((off >= fs->read_start_addr) && (len <= (fs->read_end_addr - fs->read_start_addr)))
    {   read_addr = ((uint64_t)fs->read_queue) +  off - fs->read_start_addr;
        memcpy(buf,(void*)read_addr, len);
        return 0;
    }

}

int logfs_append(struct logfs *fs, const void *buf, uint64_t len) {
    uint64_t free_space;
    if(fs->write_head < fs->write_tail) 
    {
        free_space = fs->write_tail - fs->write_head; /*Free space lies in between head (behind) and tail*/
    }
    else{
        free_space = fs->write_limit - (uint64_t)fs->write_queue - (fs->write_head - fs->write_tail); /* Enough space availale -> head in front of tail */
    }
    if(len >= free_space) {
        reader_function(fs); /*Reader function called to invoke write to empty our write buf by flushing*/
    }

    if(fs->write_head + len <= fs->write_limit){
        memcpy((void*)fs->write_head, buf, len);
        fs->write_head += len;
        fs->write_allow = 1; /* Can start flushing from write buffer to device*/
        pthread_cond_signal(&fs->can_write);
        return 0;
    }

    free_space = fs->write_limit - fs->write_head;
    memcpy((void*)fs->write_head, buf, free_space);
    len -= free_space;
    memcpy((void*)fs->write_queue, buf+free_space , len);
    fs->write_head = (uint64_t)fs->write_queue + len;
    fs->write_allow = 1; /* Can start flushing from write buffer to device*/
    pthread_cond_signal(&fs->can_write); /* Send signal to writer that data is available to flush */
    return 0;
}


int logfs_flush_(struct logfs *fs, uint64_t start, uint64_t end){
    uint64_t diff;
    void * temp_mem;
    uint64_t diff_;
    uint64_t len;
    uint64_t len_diff;

    if( start == end)
    {
        return 0;
    }
    diff = fs->device_limit % fs->block_size;
    if(diff > 0)
    { diff_ = fs->block_size - diff;
      temp_mem = aligned_alloc(fs->block_size,fs->block_size);

      memset(temp_mem, 0, fs->block_size);

      device_read(fs->device, temp_mem, (fs->device_limit - diff),fs->block_size);
      memcpy((void *)(temp_mem+diff),(void *)start, diff_);
      device_write(fs->device, temp_mem, (fs->device_limit - diff),fs->block_size);
      FREE(temp_mem);
      if((start + diff_)>end)
      {
        fs->device_limit += end - start;
        return 0;
      }else{
        fs->device_limit += diff_;
        start += diff_;                                                    
      }

    }
    if((start % fs->block_size)>0 )
    {
        temp_mem = aligned_alloc(fs->block_size,fs->block_size * WCACHE_BLOCKS);
        memset(temp_mem, 0, fs->block_size * WCACHE_BLOCKS);
        len = end - start;
        memcpy(temp_mem,(void *)start, len);
        logfs_flush_(fs, (uint64_t) temp_mem, (uint64_t)temp_mem + len);
        FREE(temp_mem);
        return 0;

    }
    len = end-start;
    len_diff = len%fs->block_size;
    if(len_diff>0)
    {
        len += fs->block_size-len_diff;
    }
    device_write(fs->device, (void*)start, fs->device_limit, len);
    fs->device_limit += end - start;
    return 0;

}

int logfs_flush(struct logfs *fs){
    uint64_t head_addr = fs->write_head;
    if(head_addr < fs->write_tail)
    {
        logfs_flush_(fs, fs->write_tail, fs->write_limit);
        logfs_flush_(fs, (uint64_t) fs->write_queue, head_addr) ;
        fs->write_tail = head_addr;
        return 0;
    }
    logfs_flush_(fs, fs->write_tail, head_addr) ;
    fs->write_tail = head_addr;
    return 0;
}

void *writer_function(void *arg) {
    struct logfs *fs = (struct logfs*)arg;


    while (!fs->should_exit) {
        pthread_mutex_lock(&fs->mutex);
        /* Program should not exit and Child Process not allowed i.e. Read not allowed then we can write */
        while ((!fs->should_exit) && !fs->write_allow) { 
            pthread_cond_wait(&fs->can_write, &fs->mutex);
        }
        logfs_flush(fs);
        fs->write_allow = 0;
        fs->read_allow = 1;
        pthread_cond_signal(&fs->can_read);
        pthread_mutex_unlock(&fs->mutex);
    }
}


void *reader_function(struct logfs *fs) {

        pthread_mutex_lock(&fs->mutex);

        fs->write_allow = 1;
        fs->read_allow = 0;
        pthread_cond_signal(&fs->can_write);

        while(!fs->read_allow){
            pthread_cond_wait(&fs->can_read, &fs->mutex);
        };
        pthread_mutex_unlock(&fs->mutex);
}
