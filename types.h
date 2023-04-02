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
#include <string>

using std::string;
using std::shared_ptr;
using std::make_shared;
using std::unique_ptr;
using std::make_unique;


#define MAX_FILE_NAME_LEN 54
#define FILE_COUNTER_THRESHOLD 1024

enum file_type: uint8_t {
    reg,
    dir
};

struct rfs_dentry_d {
    uint64_t ino;
    file_type ftype;
    char name[MAX_FILE_NAME_LEN + 1];
};

class inode_t {

private:
    uint8_t* _data; // includes used and free areas
public:
    size_t attr_sz;
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
    void overwrite_dentry_d(rfs_dentry_d *src, rfs_dentry_d* dst);
    ~inode_t();
};

//struct inode_d {
//    int dir_cnt;
//};


struct rfs_dentry {
    uint64_t ino;
    file_type ftype;
    char name[MAX_FILE_NAME_LEN + 1];

    shared_ptr<inode_t> inode;
};

// f_counter: new-created file counter, when it reaches FILE_COUNTER_THRESHOLD, write back the cur_ino to super_block
struct super_block {
    uint64_t cur_ino;
    rfs_dentry_d root_dentry;
    uint64_t f_counter;
};

struct super_block_d {
    uint64_t cur_ino;
};

struct inode_cache {
    uint32_t ref_cnt;
    string path; // only available in directory's cache
    shared_ptr<inode_t> i;
};

#endif //ROCKS_FUSE_TYPES_H
