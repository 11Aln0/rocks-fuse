//
// Created by aln0 on 3/6/23.
//

#ifndef ROCKS_FUSE_TYPES_H
#define ROCKS_FUSE_TYPES_H

enum file_type {
    reg,
    dir
};

#define SUPER_BLOCK_INO 0
#define ROOT_DENTRY_INO 1

struct inode {
    uint8_t* data;
    size_t size; // size of the data
};

//struct inode_d {
//    int dir_cnt;
//};

struct rfs_dentry_d {
    uint64_t ino;
    file_type ftype;
    char name[24];
};

struct rfs_dentry {
    int ino;
    file_type ftype;
    char name[24];

    inode* _inode;
};

struct super_block {
    uint64_t cur_ino;
    rfs_dentry_d root_dentry;
};

struct super_block_d {
    uint64_t cur_ino;
};


#endif //ROCKS_FUSE_TYPES_H
