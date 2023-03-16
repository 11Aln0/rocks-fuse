//
// Created by aln0 on 3/6/23.
//

#ifndef ROCKS_FUSE_ROCKSDB_FS_H
#define ROCKS_FUSE_ROCKSDB_FS_H
#define FUSE_USE_VERSION 31

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

#define RFS_DEBUG(fn, msg) do {printf("RFS_DEBUG:[%s] %s\n", fn, msg); } while(0)

class rocksdb_fs {

private:
    rocksdb::DB* db;
    super_block super;
    mutex ino_lock;

private:
    inode_t* read_inode(uint64_t ino);
    void write_inode(uint64_t ino, const inode_t* inode);
    void drop_inode(uint64_t ino);

    unique_ptr<rfs_dentry> lookup(char* path, bool &found);

    rfs_dentry_d* new_dentry_d(const char* fname, file_type ftype);
    rfs_dentry_d* find_dentry(rfs_dentry* parent, const char* name);
    void add_dentry_d(rfs_dentry* parent, rfs_dentry_d* dentry_d);
    void drop_dentry_d(rfs_dentry* parent, rfs_dentry_d* dentry_d);
    void drop_dentry_d(const rfs_dentry_d* dentry_d);

    char* parent_path(const char* path, int& div_idx);

public:
    int connect(const char *dbpath);
    int mount();
    int close();
    int mkdir(const char* path);
    int rmdir(const char* path);
    int getattr(const char* path, struct stat* stat);
    int readdir(const char* path, void* buf, fuse_fill_dir_t filter);
    int mknod(const char* path, mode_t mode);
    int unlink(const char*path);
    int write(const char* path, const char* buf, size_t size, off_t offset);
    int read(const char* path, char* buf, size_t size, off_t offset);
};


#endif //ROCKS_FUSE_ROCKSDB_FS_H
