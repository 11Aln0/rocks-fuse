//
// Created by aln0 on 3/6/23.
//

#ifndef ROCKS_FUSE_TYPES_H
#define ROCKS_FUSE_TYPES_H

#define SUPER_BLOCK_INO 0
#define ROOT_DENTRY_INO 1

#include<memory>
#include <cstring>
#include <cstdio>

using std::shared_ptr;
using std::make_shared;
using std::unique_ptr;
using std::make_unique;

enum file_type {
    reg,
    dir
};

struct rfs_dentry_d {
    uint64_t ino;
    file_type ftype;
    char name[24];
};

class inode_t {

private:
    uint8_t* _data;
public:
    size_t d_size; // d_size of the data
//    size_t f_size; // d_size of the whole inode
public:
    inode_t(void* data, size_t size);
    const uint8_t* data() const;
    void write_data(const char* buf, size_t size, off_t offset);
    void append_dentry_d(rfs_dentry_d *d);
    void drop_dentry_d(rfs_dentry_d *d);
    ~inode_t();
};

//struct inode_d {
//    int dir_cnt;
//};


struct rfs_dentry {
    int ino;
    file_type ftype;
    char name[24];

    unique_ptr<inode_t> inode;
};

struct super_block {
    uint64_t cur_ino;
    rfs_dentry_d root_dentry;
};

struct super_block_d {
    uint64_t cur_ino;
};


#endif //ROCKS_FUSE_TYPES_H
