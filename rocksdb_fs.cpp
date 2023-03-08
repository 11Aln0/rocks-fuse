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
    Status s = db->Get(ReadOptions(), "0", &rV); // root dir entry resides in inode 0
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
        s = db->Put(WriteOptions(), "1", Slice()); // write root dir inode
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
    rfs_dentry* last_dentry = lookup(path, &found);
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
    return 0;

}

int rocksdb_fs::getattr(const char *path, struct stat *stat) {
    bool found;
    rfs_dentry* dentry = lookup(path, &found);
    if(!found) {
        return -ENOENT;
    }

    if(dentry->ftype == dir) {
        stat->st_mode = S_IFDIR | 0777;
    } else {
        stat->st_mode = S_IFREG | 0777;
    }

    stat->st_nlink = 1;
    stat->st_size = dentry->size;

    free_dentry(dentry);
    return 0;
}

int rocksdb_fs::readdir(const char *path, void *buf, fuse_fill_dir_t filter) {
    bool found;
    rfs_dentry* dentry = lookup(path, &found);
    if(!found) {
        return -ENOENT;
    }

    if(dentry->ftype == reg) {
        return -ENOTDIR;
    }

    rfs_dentry_d* dentry_cursor = (rfs_dentry_d*) dentry->inode->data;
    size_t dir_cnt = dentry->size / sizeof(rfs_dentry_d);
    struct stat stat = {};
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

    free_dentry(dentry);

    return 0;
}

int rocksdb_fs::mknod(const char *path, mode_t mode) {
    bool found;
    rfs_dentry* last_dentry = lookup(path, &found);
    if (found) {
        return -EEXIST;
    }

    // get file name
    int i = strlen(path);
    while(i >= 0 && path[i] != '/') i--;

    rfs_dentry_d* dentry_d;

    if(mode & S_IFREG) {
        dentry_d = new_dentry_d(path + i + 1, reg);
    } else {
        dentry_d = new_dentry_d(path + i + 1, dir);
    }

    add_dentry_d(last_dentry, dentry_d);

    free_dentry(last_dentry);
    delete dentry_d;

    return 0;

}

int rocksdb_fs::write(const char *path, const char *buf, size_t size, off_t offset) {
    bool found;
    rfs_dentry* parent_dentry = lookup(path, &found, true);
    if(!found) {
        return -ENOENT;
    }

    if(parent_dentry->ftype != dir) {
        return -ENOTDIR;
    }


    



}


