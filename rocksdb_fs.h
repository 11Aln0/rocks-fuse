//
// Created by aln0 on 3/6/23.
//

#ifndef ROCKS_FUSE_ROCKSDB_FS_H
#define ROCKS_FUSE_ROCKSDB_FS_H

#include "rocksdb/db.h"
#include "fuse.h"
#include "types.h"
#include <string>
#include <mutex>

using std::mutex;
using std::string;
using rocksdb::Status;
using rocksdb::Slice;
using rocksdb::ReadOptions;
using rocksdb::WriteOptions;

#define RFS_DEBUG(fn, msg) do {printf("RFS_DEBUG:[%s] %s", fn, msg); } while(0)


class rocksdb_fs {
private:
    rocksdb::DB* db;
    super_block super;
    mutex ino_lock;

private:
    inode* read_inode(int ino);
    rfs_dentry* lookup(const char* path, bool *found);
    rfs_dentry_d* new_dentry_d(const char* fname, file_type ftype);
    void add_dentry_d(rfs_dentry* parent, rfs_dentry_d* dentry_d);
    void free_dentry(rfs_dentry* dentry);
public:
    int connect(const char *dbpath);
    int mount();
    int close();
    int mkdir(const char* path);
    int getattr(const char* path, struct stat* stat);
    int readdir(const char* path, void* buf, fuse_fill_dir_t filter);
    int mknod(const char* path, mode_t mode, dev_t dev);

};


#endif //ROCKS_FUSE_ROCKSDB_FS_H
