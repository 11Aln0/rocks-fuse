//
// Created by aln0 on 3/6/23.
//

#include "rocksdb_fs.h"
#include "types.h"

int rocksdb_fs::connect(const char *dbpath) {
    rocksdb::Options options;
    options.IncreaseParallelism();
    options.OptimizeLevelStyleCompaction();
    options.create_if_missing = true;
    Status s = rocksdb::DB::Open(options, dbpath, &db);
    if(!s.ok()) {
        RFS_DEBUG("rfs::connect", "DB connection failed");
        return -1;
    }
    return 0;
}

int rocksdb_fs::mount() {
    if(db == nullptr) {
        return -1;
    }
    string rV;
    Status s = db->Get(ReadOptions(), "0", &rV); // root dir entry resides in _inode 0
    super_block_d* super_d;
    if(s.code() == Status::Code::kNotFound) {
        // mounted for the first time
        super_d = new super_block_d;
        super_d->cur_ino = 1;
        s = db->Put(WriteOptions(), "0", Slice((char*)(super_d), sizeof(super_block_d))); // write super block
        if(!s.ok()) {
            RFS_DEBUG("rfs::mount", "fs init failed");
            return -1;
        }
        size_t size = 0;
        s = db->Put(WriteOptions(), "1", Slice((char*)(&size), sizeof(size_t))); // write root dir _inode
        if(!s.ok()) {
            RFS_DEBUG("rfs::mount", "fs init failed");
            return -1;
        }
    } else {
        super_d = (super_block_d*)(rV.data());
    }
    super.cur_ino = super_d->cur_ino;
    super.root_dentry.ftype = file_type::dir;
    super.root_dentry.ino = 1;
    strcpy(super.root_dentry.name, "/");
    if(super_d != (super_block_d*)rV.data()) delete super_d;
    return 0;
}

int rocksdb_fs::close() {
    Status s = db->Close();
    if(!s.ok()) {
        return -1;
    }
    return 0;
}

int rocksdb_fs::mkdir(const char* path) {
    bool found;
    char* path_cpy = strdup(path);
    rfs_dentry* last_dentry = lookup(path_cpy, found);
    if(found) {
        return -EEXIST;
    }
    if(last_dentry->ftype == reg) {
        return -ENOTDIR;
    }

    // get file name
    int i = strlen(path);
    while(i >= 0 && path[i] != '/') i--;
    rfs_dentry_d* dentry_d = new_dentry_d(path + i + 1, dir);
    add_dentry_d(last_dentry, dentry_d);

    free_dentry(last_dentry);
    delete dentry_d;
    delete path_cpy;
    return 0;

}

int rocksdb_fs::getattr(const char *path, struct stat *stat) {
    int ret = 0;
    bool found;

    char* path_cpy = strdup(path);
    rfs_dentry* dentry = lookup(path_cpy, found);
    if(!found) {
        ret = -ENOENT;
        goto defer;
    }

    if(dentry->ftype == dir) {
        stat->st_mode = S_IFDIR | 0777;
    } else {
        stat->st_mode = S_IFREG | 0777;
    }

    stat->st_nlink = 1;
    stat->st_size = dentry->_inode->size;

    defer:
    free_dentry(dentry);
    delete path_cpy;
    return ret;
}

int rocksdb_fs::readdir(const char *path, void *buf, fuse_fill_dir_t filter) {
    int ret = 0;
    bool found;
    rfs_dentry_d* dentry_cursor;
    size_t dir_cnt;
    struct stat stat = {};
    inode* cur_inode;

    char* path_cpy = strdup(path);
    rfs_dentry* dentry = lookup(path_cpy, found);
    if(!found) {
        ret = -ENOENT;
        goto defer;
    }

    if(dentry->ftype == reg) {
        ret = -ENOTDIR;
        goto defer;
    }

    dentry_cursor = (rfs_dentry_d*) dentry->_inode->data;
    dir_cnt = dentry->_inode->size / sizeof(rfs_dentry_d);
    for(int i = 0;i < dir_cnt;i++, dentry_cursor++) {
        if(dentry->ftype == dir) {
            stat.st_mode = S_IFDIR | 0777;
        } else {
            stat.st_mode = S_IFREG | 0777;
        }
        cur_inode = read_inode(dentry->ino);
        stat.st_nlink = 1;
        stat.st_size = cur_inode->size;
        delete cur_inode->data;
        delete cur_inode;
        filter(buf, dentry_cursor->name, &stat, 0, FUSE_FILL_DIR_PLUS);
    }

    defer:
    free_dentry(dentry);
    delete []path_cpy;
    return ret;
}

int rocksdb_fs::mknod(const char *path, mode_t mode) {
    int ret = 0;
    bool found;
    rfs_dentry_d* dentry_d = nullptr;

    int i = strlen(path);
    char* path_cpy = strdup(path);
    rfs_dentry* last_dentry = lookup(path_cpy, found);
    if (found) {
        ret = -EEXIST;
        goto defer;
    }

    // get file name
    while(i >= 0 && path[i] != '/') i--;

    if(mode & S_IFREG) {
        dentry_d = new_dentry_d(path + i + 1, reg);
    } else {
        dentry_d = new_dentry_d(path + i + 1, dir);
    }

    add_dentry_d(last_dentry, dentry_d);

    defer:
    free_dentry(last_dentry);
    delete dentry_d;
    delete []path_cpy;
    return ret;

}

int rocksdb_fs::write(const char* path, const char *buf, size_t size, off_t offset) {
    int ret;
    bool found;
    inode* target_inode = nullptr;
    rfs_dentry_d* target_dentry;

    int k;
    char* p_path = parent_path(path, k);
    rfs_dentry* parent_dentry = lookup(p_path, found);
    if(!found) {
        ret = -ENOENT;
        goto defer;
    }

    if(parent_dentry->ftype != dir) {
        ret = -ENOTDIR;
        goto defer;
    }

    target_dentry = find_dentry(parent_dentry, path + k + 1);
    if(target_dentry == nullptr) {
        ret = -ENOENT;
        goto defer;
    }

    if(target_dentry->ftype == dir) {
        ret = -EISDIR;
        goto defer;
    }

    size = strlen(buf);
    target_inode = read_inode(target_dentry->ino);
    if(offset > target_inode->size) {
        ret = 0;
        goto defer;
    }
    target_inode->size = offset + size > target_inode->size ? offset + size : target_inode->size;
    if(target_inode->size> 1 << 12) {
        ret = -EFBIG;
        goto defer;
    }

    target_inode->data = (uint8_t *)realloc(target_inode->data, target_inode->size);
    memcpy(target_inode->data + offset, buf, size);
    write_inode(target_dentry->ino, target_inode);
    ret = size;

    defer:
    delete []p_path;
    delete target_inode->data;
    delete target_inode;
    free_dentry(parent_dentry);
    return ret;
}

int rocksdb_fs::read(const char *path, char *buf, size_t size, off_t offset) {
    int ret;
    bool found;
    char* path_cpy = strdup(path);
    rfs_dentry* dentry = lookup(path_cpy, found);

    if(!found) {
        ret = -ENOENT;
        goto defer;
    }

    if(dentry->ftype == dir) {
        ret = -EISDIR;
        goto defer;
    }

    if(offset >= dentry->_inode->size) {
        ret = 0;
        goto defer;
    }

    size = std::min(dentry->_inode->size - offset, size);
    memcpy(buf, dentry->_inode->data + offset, size);
    ret = size;

    defer:
    free_dentry(dentry);
    delete path_cpy;
    return ret;
}

int rocksdb_fs::rmdir(const char *path) {
    int ret = 0;
    bool found;
    rfs_dentry_d* target_dentry;

    int k;
    char* p_path = parent_path(path, k);

    rfs_dentry* parent_dentry = lookup(p_path, found);

    if(!found) {
        ret = -ENOENT;
        goto defer;
    }

    target_dentry = find_dentry(parent_dentry, path + k + 1);
    if(target_dentry == nullptr) {
        ret = -ENOENT;
        goto defer;
    }

    drop_dentry_d(parent_dentry, target_dentry);

    defer:
    free_dentry(parent_dentry);
    delete []p_path;
    return ret;

}


