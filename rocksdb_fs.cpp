//
// Created by aln0 on 3/6/23.
//

#include "rocksdb_fs.h"
#include "types.h"
#include <unistd.h>
#include <time.h>

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
    super.f_counter = 0;
    super.cur_ino = super_d->cur_ino + FILE_COUNTER_THRESHOLD;
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

int rocksdb_fs::mkdir(const char* path, mode_t mode) {
    uint64_t ino;
    return mknod(path, mode, &ino);
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

int rocksdb_fs::opendir(const char *path, fuse_file_info *fi) {
    bool found;
    auto path_cpy = unique_ptr<char>(strdup(path));
    auto dentry = lookup(path_cpy.get(), found);

    if(!found) {
        return -ENOENT;
    }

    if(dentry->ftype == reg) {
        return -ENOTDIR;
    }

    fi->fh = dentry->ino;
    if((fi->flags & O_DIRECT) == 0) {
        // the file has been cached
        cache_lock.lock();
        if(cache.find(fi->fh) != cache.end()) {
            cache[dentry->ino].ref_cnt++;
        } else {
            inode_cache c = {1, path, dentry->inode};
            cache[dentry->ino] = c;
            dir_cache[c.path] = dentry->inode;
        }
        cache_lock.unlock();
    }

    return 0;
}

int rocksdb_fs::releasedir(const char *path, fuse_file_info *fi) {
    cache_lock.lock();
    if(cache.find(fi->fh) != cache.end()) {
        // if cache's ref_cnt reaches to 0, the cache will be released
        if(--cache[fi->fh].ref_cnt == 0) {
            write_inode(fi->fh, cache[fi->fh].i.get());
            // release directory cache
            if(dir_cache.find(path) != dir_cache.end()) {
                dir_cache.erase(path);
            }
            cache.erase(fi->fh);
        }
    }
    cache_lock.unlock();
    return 0;
}

int rocksdb_fs::readdir(const char* path, void* buf, fuse_fill_dir_t filter,
                        off_t off, struct fuse_file_info* fi, fuse_readdir_flags flags) {
    bool found;
    bool lock = false;
    shared_ptr<inode_t> dir_inode;
    cache_lock.lock_shared();
    if(cache.find(fi->fh) != cache.end()) {
        dir_inode = cache[fi->fh].i;
        lock = true;
    } else {
        cache_lock.unlock_shared();
        auto path_cpy = unique_ptr<char>(strdup(path));
        auto dentry = lookup(path_cpy.get(), found);
        if(!found) {
            return -ENOENT;
        }

        if(dentry->ftype == reg) {
            return -ENOTDIR;
        }

        dir_inode = dentry->inode;
    }

    auto dentry_cursor = (rfs_dentry_d*) dir_inode->data();
    dentry_cursor += off;
    unique_ptr<inode_t> cur_inode;
    size_t dir_cnt = dir_inode->used_dat_sz / sizeof(rfs_dentry_d);
    if(dir_cnt <= off) {
        if(lock) cache_lock.unlock_shared();
        return 0;
    }

    struct stat stat = {};
    int ret;
    for(off_t i = off;i < dir_cnt; dentry_cursor++, i++) {
        if(dentry_cursor->ftype == dir) {
            stat.st_mode = S_IFDIR | 0777;
        } else {
            stat.st_mode = S_IFREG | 0777;
        }
        cur_inode = unique_ptr<inode_t>(read_inode(dentry_cursor->ino));
        stat.st_nlink = 1;
        stat.st_size = cur_inode->used_dat_sz;
        stat.st_blocks = 1;
        ret = filter(buf, dentry_cursor->name, &stat, i + 1, FUSE_FILL_DIR_PLUS);
        if(ret == 1) {
            return 0;
        }
    }

    if(lock) cache_lock.unlock_shared();
    return 0;
}


int rocksdb_fs::mknod(const char *path, mode_t mode, uint64_t* ino) {
    bool found;
    int k;
    shared_ptr<inode_t> parent_inode;
    uint64_t write_back_ino = 0;
    auto path_cpy = unique_ptr<char>(strdup(path));
    auto par_path = unique_ptr<char>(parent_path(path, k));

    int f_name_len = strlen(path_cpy.get() + k + 1);
    if(f_name_len > MAX_FILE_NAME_LEN) {
        // truncate the file_name
        auto end = k + 1 + MAX_FILE_NAME_LEN;
        path_cpy.get()[end] = '\0';
    }

    cache_lock.lock();
    if(dir_cache.find(par_path.get()) != dir_cache.end()) {
        parent_inode = dir_cache[par_path.get()];
        rfs_dentry_d* target_dentry = find_dentry(parent_inode.get(), path_cpy.get() + k + 1);

        if(target_dentry != nullptr) {
            // if inode is read from cached, the cache lock needs to be released
            cache_lock.unlock();
            return -EEXIST;
        }

    } else {
        cache_lock.unlock();
        auto last_dentry = lookup(path_cpy.get(), found);

        if (found) {
            return -EEXIST;
        }

        write_back_ino = last_dentry->ino;
        parent_inode = last_dentry->inode;
    }

    unique_ptr<rfs_dentry_d> dentry_d;
    if(mode & S_IFREG) {
        dentry_d = unique_ptr<rfs_dentry_d>(new_dentry_d(path_cpy.get() + k + 1, reg));
    } else {
        dentry_d = unique_ptr<rfs_dentry_d>(new_dentry_d(path_cpy.get() + k + 1, dir));
    }

    write_inode(dentry_d->ino, nullptr);
    parent_inode->append_dentry_d(dentry_d.get());

    if(write_back_ino) {
        write_inode(write_back_ino, parent_inode.get());
    } else {
        cache_lock.unlock();
    }

    *ino = dentry_d->ino;

    return 0;
}

int rocksdb_fs::write(const char* path, const char *buf, size_t size, off_t offset, fuse_file_info* fi) {
    shared_ptr<inode_t> inode;
    unique_ptr<rfs_dentry> dentry;
    bool lock = false;

    if((fi->flags & O_DIRECT) == 0) {
        cache_lock.lock();
        inode = cache[fi->fh].i;
        lock = true;
    } else {
        bool found;
        auto path_cpy = unique_ptr<char>(strdup(path));
        dentry = lookup(path_cpy.get(), found);
        if(!found) {
            return -ENOENT;
        }
        if(dentry->ftype == dir) {
            return -EISDIR;
        }

        inode = dentry->inode;
    }

    if(offset > inode->size) {
        if(lock) cache_lock.unlock();
        return 0;
    }

    size_t n_size = offset + size;
    if(n_size > 1 << 12) {
        if(lock) cache_lock.unlock();
        return -EFBIG;
    }

    inode->write_data(buf, size, offset);

    if(fi->flags & O_DIRECT) {
        write_inode(dentry->ino, dentry->inode.get());
    } else {
        cache_lock.unlock();
    }

    return size;
}

int rocksdb_fs::read(const char *path, char *buf, size_t size, off_t offset, fuse_file_info* fi) {
    shared_ptr<inode_t> inode;
    bool lock = false;
    if((fi->flags & O_DIRECT) == 0) {
        cache_lock.lock_shared();
        inode = cache[fi->fh].i;
        lock = true;
    }
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
    }

    if(offset >= inode->size) {
        if(lock) cache_lock.unlock_shared();
        return 0;
    }

    size = std::min(inode->size - offset, size);
    memcpy(buf, inode->data() + offset, size);

    if(lock) cache_lock.unlock_shared();
    return size;
}

int rocksdb_fs::rmdir(const char *path) {
    bool found;
    int k;
    uint64_t write_back_ino = 0;
    shared_ptr<inode_t> parent_inode;
    auto p_path = unique_ptr<char>(parent_path(path, k));
    string key = p_path.get();
    cache_lock.lock();
    if(dir_cache.find(key) != dir_cache.end()) {
        parent_inode = dir_cache[key];
    } else {
        cache_lock.unlock();
        auto parent_dentry = lookup(p_path.get(), found);

        if(!found) {
            return -ENOENT;
        }

        parent_inode = parent_dentry->inode;
        write_back_ino = parent_dentry->ino;
    }


    rfs_dentry_d* target_dentry = find_dentry(parent_inode.get(), path + k + 1);

    if(target_dentry == nullptr) {
        return -ENOENT;
    }

    if(target_dentry->ftype != dir) {
        return -ENOTDIR;
    }

    drop_dentry_d(target_dentry);
    parent_inode->drop_dentry_d(target_dentry);

    if(write_back_ino) {
        write_inode(write_back_ino, parent_inode.get());
    } else {
        cache_lock.unlock();
    }

    return 0;

}

int rocksdb_fs::unlink(const char *path) {
    bool found;
    int k;
    // if write_back_ino is not equal to 0, it means that the parent inode is not cached
    uint64_t write_back_ino = 0;
    auto p_path = unique_ptr<char>(parent_path(path, k));
    shared_ptr<inode_t> parent_inode;

    cache_lock.lock();
    if(dir_cache.find(p_path.get()) != dir_cache.end()) {
        parent_inode = dir_cache[p_path.get()];
    } else {
        cache_lock.unlock();
        auto parent_dentry = lookup(p_path.get(), found);
        write_back_ino = parent_dentry->ino;

        if(!found) {
            return -ENOENT;
        }

        parent_inode = parent_dentry->inode;
    }

    rfs_dentry_d* target_dentry = find_dentry(parent_inode.get(), path + k + 1);

    if(target_dentry == nullptr) {
        // if inode is read from cached, the cache lock needs to be released
        if(!write_back_ino) cache_lock.unlock();
        return -ENOENT;
    }

    drop_inode(target_dentry->ino);
    parent_inode->drop_dentry_d(target_dentry);

    if(write_back_ino) {
        write_inode(write_back_ino, parent_inode.get());
    } else {
        cache_lock.unlock();
    }

    return 0;
}

int rocksdb_fs::rename(const char* src, const char* dst) {
    if(strcmp(src, dst) == 0) {
        return 0;
    }

    // get source parent directory entry and source file directory entry
    bool found;
    int k;
    auto src_parent_path = unique_ptr<char>(parent_path(src, k));
    auto src_parent_dentry = lookup(src_parent_path.get(), found);

    if(!found) {
        return -ENOENT;
    }

    rfs_dentry_d* src_file_dentry = find_dentry(src_parent_dentry->inode.get(), src + k + 1);
    if(src_file_dentry == nullptr) {
        return -ENOENT;
    }

    // get destination parent directory entry and destination file directory entry
    auto dst_parent_path = unique_ptr<char>(parent_path(dst, k));
    // judge if the operation is just renaming
    if(strcmp(src_parent_path.get(), dst_parent_path.get()) == 0) {
        strcpy(src_file_dentry->name, dst + k + 1);
        write_inode(src_parent_dentry->ino, src_parent_dentry->inode.get());
        return 0;
    }

    auto dst_parent_dentry = lookup(dst_parent_path.get(), found);
    if(!found) {
        return -ENOENT;
    }

    // process destination
    rfs_dentry_d* dst_file_dentry = find_dentry(dst_parent_dentry->inode.get(), dst + k + 1);
    if(dst_file_dentry == nullptr) {
        dst_parent_dentry->inode->append_dentry_d(src_file_dentry);
        write_inode(dst_parent_dentry->ino, dst_parent_dentry->inode.get());
    } else {
        overwrite_dentry_d(dst_parent_dentry.get(), src_file_dentry, dst_file_dentry);
    }

    src_parent_dentry->inode->drop_dentry_d(src_file_dentry);
    write_inode(src_parent_dentry->ino, src_parent_dentry->inode.get());

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

    fi->fh = dentry->ino;
    if((fi->flags & O_DIRECT) == 0) {
        // the file has been cached
        cache_lock.lock();
        if(cache.find(fi->fh) != cache.end()) {
            cache[dentry->ino].ref_cnt++;
        } else {
            inode_cache c = {1, "", dentry->inode};
            cache[dentry->ino] = c;
        }
        cache_lock.unlock();
    }

    return 0;

}

int rocksdb_fs::create(const char *path, mode_t mode, fuse_file_info *fi) {
    uint64_t ino;
    int ret = mknod(path, mode, &ino);
    if(ret < 0) {
        return ret;
    }

    fi->fh = ino;

    if((fi->flags & O_DIRECT) == 0) {
        cache_lock.lock();
        inode_cache c = {1, "", shared_ptr<inode_t>(read_inode(ino))};
        cache[ino] = c;
        cache_lock.unlock();
    }

    return 0;
}

int rocksdb_fs::fsync(fuse_file_info *fi) {
    cache_lock.lock();
    if(cache.find(fi->fh) != cache.end()) {
        write_inode(fi->fh, cache[fi->fh].i.get());
    }
    cache_lock.unlock();
    return 0;
}

int rocksdb_fs::release(fuse_file_info *fi) {
    cache_lock.lock();
    if(cache.find(fi->fh) != cache.end()) {
        // if cache's ref_cnt reaches to 0, the cache will be released
        if(--cache[fi->fh].ref_cnt == 0) {
            write_inode(fi->fh, cache[fi->fh].i.get());
            cache.erase(fi->fh);
        }
    }
    cache_lock.unlock();
    return 0;
}

int rocksdb_fs::truncate(const char *path, off_t size, struct fuse_file_info *fi) {
    shared_ptr<inode_t> inode;
    unique_ptr<rfs_dentry> dentry;

    cache_lock.lock();
    if((fi->flags & O_DIRECT) == 0) {
        inode = cache[fi->fh].i;
    }
    cache_lock.unlock();

    // cache miss
    if(inode == nullptr) {
        bool found;
        auto path_cpy = unique_ptr<char>(strdup(path));
        dentry = lookup(path_cpy.get(), found);

        if(!found) {
            return -ENOENT;
        }

        if(dentry->ftype == dir) {
            return -EISDIR;
        }
        inode = dentry->inode;
    }

    inode->truncate(size);

    if(fi->flags & O_DIRECT) {
        write_inode(dentry->ino, dentry->inode.get());
    }

    return 0;
}




