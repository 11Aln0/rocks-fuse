//
// Created by aln0 on 3/7/23.
//

#include "rocksdb_fs.h"

/**
 * @return  return nullptr means that an inode has corrupted
 */
inode* rocksdb_fs::read_inode(int ino) {
    string rV;
    char key[20];
    sprintf(key, "%d", ino);
    Status s = db->Get(ReadOptions(), key, &rV);
    if(!s.ok()) {
        RFS_DEBUG("rfs::read_inode", "retrieve inode failed!");
        return nullptr;
    }

    auto ret = new inode;
    ret->data = new uint8_t[rV.size()];
    memcpy(ret->data, rV.c_str(), rV.size());

    return ret;

}

/**
 * @param path
 * @return the last directory entry that can be retrieved
 * returning nullptr means that a directory has corrupted
 */
rfs_dentry* rocksdb_fs::lookup(const char *path, bool* found) {

    rfs_dentry* dentry_ret = new rfs_dentry;

    // copy root directory entry to ret
    dentry_ret->ftype = dir;
    strcpy(dentry_ret->name, "/");
    dentry_ret->ino = ROOT_DENTRY_INO;
    dentry_ret->inode = read_inode(ROOT_DENTRY_INO);

    // if path is root path
    if(path[0] == '/' && strlen(path) == 1) {
        return dentry_ret;
    }

    char* path_cpy = new char[sizeof(path)];
    strcpy(path_cpy, path);
    char* dir_name = strtok(path_cpy, "/");
    *found = true;

    rfs_dentry_d* dentry_cursor;
    inode* i;
    int k;
    size_t dir_cnt;

    while(dir_name) {
        if(dentry_ret->ftype == dir) {
            dentry_cursor = (rfs_dentry_d*)(dentry_ret->inode->data);
            dir_cnt = dentry_ret->size / sizeof(rfs_dentry_d);
            for(k = 0;k < dir_cnt;k++, dentry_cursor++) {
                if(strcmp(dentry_cursor->name, dir_name) == 0) {
                    break;
                }
            }
            if(k == dir_cnt) {
                RFS_DEBUG("rfs::lookup", "directory not found");
                *found = false;
                break;
            }
        } else {
            // not a directory, still return the directory entry
            *found = false;
            break;
        }

        dentry_ret->ftype = dentry_cursor->ftype;
        dentry_ret->size = dentry_cursor->size;
        dentry_ret->ino = dentry_cursor->ino;
        strcpy(dentry_ret->name, dentry_cursor->name);
        delete []dentry_ret->inode->data;
        delete dentry_ret->inode;
        i = read_inode(dentry_ret->ino);
        // corrupted
        if(i == nullptr) {
            delete dentry_ret;
            return nullptr;
        }
        dentry_ret->inode = i;

        dir_name = strtok(nullptr, "/");
    }
    return dentry_ret;
}

rfs_dentry_d* rocksdb_fs::new_dentry_d(const char* fname, file_type ftype) {
    auto ret = new rfs_dentry_d;
    ret->ftype = ftype;
    strcpy(ret->name, fname);
    ino_lock.lock();
    ret->ino = ++super.cur_ino;
    db->Put(WriteOptions(), "0", Slice((char*)&super, sizeof(super_block_d))); // may exist someway faster
    ino_lock.unlock();
    return ret;
}

void rocksdb_fs::add_dentry_d(rfs_dentry* parent, rfs_dentry_d *dentry_d) {
    // update parent dentry
    parent->size += sizeof(rfs_dentry_d);
    parent->inode->data = (uint8_t*)realloc(parent->inode->data, parent->size);
    size_t dir_cnt = parent->size / sizeof(rfs_dentry_d);
    memcpy((rfs_dentry_d*)parent->inode->data + dir_cnt - 1, dentry_d, sizeof(rfs_dentry_d));

    // write back to parent inode
    char key[20];
    sprintf(key, "%d", parent->ino);
    db->Put(WriteOptions(), key, Slice((char*)parent->inode->data, parent->size));
    // write back to new inode
    sprintf(key, "%d", dentry_d->ino);
    db->Put(WriteOptions(), key, Slice());

}

void rocksdb_fs::free_dentry(rfs_dentry *dentry) {
    delete dentry->inode->data;
    delete dentry->inode;
    delete dentry;
}