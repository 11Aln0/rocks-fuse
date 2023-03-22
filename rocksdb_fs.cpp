//
// Created by aln0 on 3/6/23.
//

#include "rocksdb_fs.h"
#include "types.h"
#include <unistd.h>

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
        int ret = write_inode(1, nullptr);
        if(ret != 0) {
            return ret;
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
    auto path_cpy = unique_ptr<char>(strdup(path));
    auto last_dentry = lookup(path_cpy.get(), found);
    if(found) {
        return -EEXIST;
    }
    if(last_dentry->ftype == reg) {
        return -ENOTDIR;
    }

    // get file name
    int i = strlen(path);
    while(i >= 0 && path[i] != '/') i--;
    auto dentry_d = unique_ptr<rfs_dentry_d>(new_dentry_d(path + i + 1, dir));
    add_dentry_d(last_dentry.get(), dentry_d.get());

    return 0;
}

int rocksdb_fs::getattr(const char *path, struct stat *stat) {
    bool found;

    auto path_cpy = unique_ptr<char>(strdup(path));
    auto dentry = lookup(path_cpy.get(), found);
    if(!found) {
        return  -ENOENT;
    }

    if(dentry->ftype == dir) {
        stat->st_mode = S_IFDIR | 0777;
    } else {
        stat->st_mode = S_IFREG | 0777;
    }
    stat->st_blocks = 1;
    stat->st_gid = getgid();
    stat->st_uid = getuid();
    stat->st_nlink = 1;
    stat->st_size = dentry->inode->used_dat_sz;

    return 0;
}

int rocksdb_fs::readdir(const char *path, void *buf, fuse_fill_dir_t filter) {
    bool found;
    auto path_cpy = unique_ptr<char>(strdup(path));
    auto dentry = lookup(path_cpy.get(), found);

    if(!found) {
        return -ENOENT;
    }

    if(dentry->ftype == reg) {
        return -ENOTDIR;
    }

    auto dentry_cursor = (rfs_dentry_d*) dentry->inode->data();
    unique_ptr<inode_t> cur_inode;
    size_t dir_cnt = dentry->inode->used_dat_sz / sizeof(rfs_dentry_d);
    struct stat stat = {};

    for(int i = 0;i < dir_cnt; dentry_cursor++) {
        if(dentry->ftype == dir) {
            stat.st_mode = S_IFDIR | 0777;
        } else {
            stat.st_mode = S_IFREG | 0777;
        }
        cur_inode = unique_ptr<inode_t>(read_inode(dentry->ino));
        stat.st_nlink = 1;
        stat.st_size = cur_inode->used_dat_sz;
        stat.st_blocks = 1;
        filter(buf, dentry_cursor->name, &stat, 0, FUSE_FILL_DIR_PLUS);
        i++;
    }

    return 0;
}


int rocksdb_fs::mknod(const char *path, mode_t mode, uint64_t* ino) {
    bool found;
    int i = strlen(path);
    auto path_cpy = unique_ptr<char>(strdup(path));
    auto last_dentry = lookup(path_cpy.get(), found);

    if (found) {
        return -EEXIST;
    }

    // get file name index
    while(i >= 0 && path[i] != '/') i--;

    unique_ptr<rfs_dentry_d> dentry_d;
    if(mode & S_IFREG) {
        dentry_d = unique_ptr<rfs_dentry_d>(new_dentry_d(path + i + 1, reg));
    } else {
        dentry_d = unique_ptr<rfs_dentry_d>(new_dentry_d(path + i + 1, dir));
    }

    add_dentry_d(last_dentry.get(), dentry_d.get());

    *ino = dentry_d->ino;

    return 0;
}

int rocksdb_fs::write(const char* path, const char *buf, size_t size, off_t offset, fuse_file_info* fi) {
    shared_ptr<inode_t> inode = cache[fi->fh];
    if(inode == nullptr) {
        bool found;
        auto path_cpy = unique_ptr<char>(strdup(path));
        auto dentry = lookup(path_cpy.get(), found);
        if(!found) {
            return -ENOENT;
        }
        if(dentry->ftype == dir) {
            return -EISDIR;
        }

        inode = dentry->inode;
        cache[fi->fh] = dentry->inode;
    }


    if(offset > inode->used_dat_sz) {
        return 0;
    }

    size_t n_size = offset + size;
    if(n_size > 1 << 12) {
        return -EFBIG;
    }

    inode->write_data(buf, size, offset);

    return size;
}

int rocksdb_fs::read(const char *path, char *buf, size_t size, off_t offset, fuse_file_info* fi) {
    shared_ptr<inode_t> inode = cache[fi->fh];
    // cache miss
    if(inode == nullptr) {
        bool found;
        auto path_cpy = unique_ptr<char>(strdup(path));
        auto dentry = lookup(path_cpy.get(), found);

        if(!found) {
            return -ENOENT;
        }

        if(dentry->ftype == dir) {
            return -EISDIR;
        }
        inode = dentry->inode;
        cache[fi->fh] = dentry->inode;
    }


    if(offset >= inode->used_dat_sz) {
        return 0;
    }

    size = std::min(inode->used_dat_sz - offset, size);
    memcpy(buf, inode->data() + offset, size);
    return size;
}

int rocksdb_fs::rmdir(const char *path) {
    return unlink(path);
}

int rocksdb_fs::unlink(const char *path) {
    bool found;
    int k;
    auto p_path = unique_ptr<char>(parent_path(path, k));
    auto parent_dentry = lookup(p_path.get(), found);

    if(!found) {
        return -ENOENT;
    }

    rfs_dentry_d* target_dentry = find_dentry(parent_dentry.get(), path + k + 1);
    if(target_dentry == nullptr) {
        return -ENOENT;
    }

    drop_dentry_d(parent_dentry.get(), target_dentry);

    return 0;
}


int rocksdb_fs::open(const char *path, struct fuse_file_info* fi) {
    bool found;
    auto path_cpy = unique_ptr<char>(strdup(path));
    auto dentry = lookup(path_cpy.get(), found);

    if(!found) {
        return -ENOENT;
    }

    if(dentry->ftype == dir) {
        return -EISDIR;
    }

    if(cache[dentry->ino] != nullptr) {
        // the file has been opened
        return -EACCES;
    }

    fi->fh = dentry->ino;
    fi->keep_cache = 1;
    cache[dentry->ino] = dentry->inode;

    return 0;

}

int rocksdb_fs::create(const char *path, mode_t mode, fuse_file_info *fi) {
    uint64_t ino;
    int ret = mknod(path, mode, &ino);
    if(ret < 0) {
        return ret;
    }

    cache[ino] = shared_ptr<inode_t>(read_inode(ino));
    fi->fh = ino;

    return 0;
}

int rocksdb_fs::fsync(fuse_file_info *fi) {
    auto inode = cache[fi->fh];
    if(inode != nullptr) {
        write_inode(fi->fh, inode.get());
    }
    return 0;
}

int rocksdb_fs::release(fuse_file_info *fi) {
    auto inode = cache[fi->fh];
    if(cache[fi->fh] != nullptr) {
        write_inode(fi->fh, inode.get());
        cache[fi->fh] = nullptr;
    }
    return 0;
}

int rocksdb_fs::truncate(const char *path, off_t size, struct fuse_file_info *fi) {
    shared_ptr<inode_t> inode = cache[fi->fh];
    // cache miss
    if(inode == nullptr) {
        bool found;
        auto path_cpy = unique_ptr<char>(strdup(path));
        auto dentry = lookup(path_cpy.get(), found);

        if(!found) {
            return -ENOENT;
        }

        if(dentry->ftype == dir) {
            return -EISDIR;
        }
        inode = dentry->inode;
        cache[fi->fh] = dentry->inode;
    }

    inode->truncate(size);

    return 0;
}




