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
    char name[20];
};

class inode_t {

private:
    uint8_t* _data; // includes used and free areas
    size_t attr_sz;
public:
    size_t size; // size of the whole inode, which is sizeof(data) + sizeof(size_t)
    size_t used_dat_sz; // size of the used data areas, the persistent attributes begins here(not including used_dat_sz)

public:
    inode_t();
    inode_t(const char* data, size_t size);
    const uint8_t* data() const;
    void before_write_back();

    void write_data(const char* buf, size_t size, off_t offset);
    void truncate(size_t size);

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

    shared_ptr<inode_t> inode;
};

struct super_block {
    uint64_t cur_ino;
    rfs_dentry_d root_dentry;
};

struct super_block_d {
    uint64_t cur_ino;
};


#endif //ROCKS_FUSE_TYPES_H
