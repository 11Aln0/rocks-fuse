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
        s = db->Put(WriteOptions(), "1", Slice()); // write root dir _inode
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
    delete super_d;
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
    rfs_dentry* last_dentry = lookup(path_cpy, &found);
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
    rfs_dentry* dentry = lookup(path_cpy, &found);
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
    stat->st_size = dentry->size;

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

    char* path_cpy = strdup(path);
    rfs_dentry* dentry = lookup(path_cpy, &found);
    if(!found) {
        ret = -ENOENT;
        goto defer;
    }

    if(dentry->ftype == reg) {
        ret = -ENOTDIR;
        goto defer;
    }

    dentry_cursor = (rfs_dentry_d*) dentry->_inode->data;
    dir_cnt = dentry->size / sizeof(rfs_dentry_d);
    for(int i = 0;i < dir_cnt;i++) {
        if(dentry->ftype == dir) {
            stat.st_mode = S_IFDIR | 0777;
        } else {
            stat.st_mode = S_IFREG | 0777;
        }
        stat.st_nlink = 1;
        stat.st_size = dentry->size;
        filter(buf, dentry_cursor->name, &stat, 0, FUSE_FILL_DIR_PLUS);
    }

    defer:
    free_dentry(dentry);
    delete path_cpy;
    return ret;
}

int rocksdb_fs::mknod(const char *path, mode_t mode) {
    int ret = 0;
    bool found;
    rfs_dentry_d* dentry_d = nullptr;

    int i = strlen(path);
    char* path_cpy = strdup(path);
    rfs_dentry* last_dentry = lookup(path_cpy, &found);
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
    delete path_cpy;
    return ret;

}

int rocksdb_fs::write(const char *path, const char *buf, size_t size, off_t offset) {
    int ret, i_fname;
    bool found;
    inode* target_inode = nullptr;
    rfs_dentry_d* dentry_cursor;


    // remove the filename
    int k = strlen(path);
    while(k >= 0 && path[k] != '/') k--;
    char* parent_path = new char[k + 1];
    memcpy(parent_path, path, k + 1);
    parent_path[k] = '\0';

    rfs_dentry* parent_dentry = lookup(parent_path, &found);
    if(!found) {
        ret = -ENOENT;
        goto defer;
    }

    if(parent_dentry->ftype != dir) {
        ret = -ENOTDIR;
        goto defer;
    }

    // get target file directory entry
    dentry_cursor = (rfs_dentry_d*)parent_dentry->_inode;
    i_fname = parent_dentry->size / sizeof(rfs_dentry_d);
    for(;i_fname > 0;i_fname--, dentry_cursor++) {
        if(strcmp(dentry_cursor->name, path + k + 1) == 0) {
            break;
        }
    }

    if(i_fname == 0) {
        ret = -ENOENT;
        goto defer;
    }

    if(dentry_cursor->ftype == dir) {
        ret = -EISDIR;
        goto defer;
    }

    dentry_cursor->size = offset + size > dentry_cursor->size ? offset + size : dentry_cursor->size;
    if(dentry_cursor->size > 1 << 12) {
        ret = -EFBIG;
        goto defer;
    }

    //
    target_inode = read_inode(dentry_cursor->ino);
    memcpy(target_inode->data + offset, buf, size);
    write_inode(dentry_cursor->ino, target_inode, dentry_cursor->size);
    write_inode(parent_dentry->ino, parent_dentry->_inode, parent_dentry->size);
    ret = dentry_cursor->size;

    defer:
    delete []parent_path;
    delete target_inode;
    free_dentry(parent_dentry);
    return ret;
}

int rocksdb_fs::read(const char *path, char *buf, size_t size, off_t offset) {
    int ret;
    bool found;
    char* path_cpy = strdup(path);
    rfs_dentry* dentry = lookup(path_cpy, &found);

    if(!found) {
        ret = -ENOENT;
        goto defer;
    }

    if(dentry->ftype == dir) {
        ret = -EISDIR;
        goto defer;
    }

    if(offset >= dentry->size) {
        ret = 0;
        goto defer;
    }

    size = std::min(dentry->size - offset, size);
    memcpy(buf, dentry->_inode->data + offset, size);
    ret = size;

    defer:
    free_dentry(dentry);
    delete path_cpy;
    return ret;
}

int rocksdb_fs::rmdir(const char *path) {
    int ret, i_fname;
    bool found;
    rfs_dentry_d* dentry_cursor;

    // remove the filename
    int k = strlen(path);
    while(k >= 0 && path[k] != '/') k--;
    char* parent_path = new char[k + 1];
    memcpy(parent_path, path, k + 1);
    parent_path[k] = '\0';

    rfs_dentry* parent_dentry = lookup(parent_path, &found);

    if(!found) {
        ret = -ENOENT;
        goto defer;
    }

    // get target file directory entry
    dentry_cursor = (rfs_dentry_d*)parent_dentry->_inode;
    i_fname = parent_dentry->size / sizeof(rfs_dentry_d);
    for(;i_fname > 0;i_fname--, dentry_cursor++) {
        if(strcmp(dentry_cursor->name, path + k + 1) == 0) {
            break;
        }
    }

    if(i_fname == 0) {
        ret = -ENOENT;
        goto defer;
    }

    drop_dentry_d(parent_dentry, dentry_cursor);

    defer:
    free_dentry(parent_dentry);
    delete []parent_path;
    return ret;

}


