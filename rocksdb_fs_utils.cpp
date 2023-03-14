//
// Created by aln0 on 3/7/23.
//

#include "rocksdb_fs.h"

/**
 * @return  return nullptr means that an _inode has corrupted
 */
inode* rocksdb_fs::read_inode(uint64_t ino) {
    string rV;
    char key[20];
    sprintf(key, "%lu", ino);
    Status s = db->Get(ReadOptions(), key, &rV);
    if(!s.ok()) {
        RFS_DEBUG("rfs::read_inode", "retrieve _inode failed!");
        return nullptr;
    }

    auto ret = new inode;
    size_t attr_size = sizeof(inode) - sizeof(uint8_t*);
    ret->size = rV.size() - attr_size;
    ret->data = new uint8_t[ret->size];
    memcpy(ret->data, rV.c_str(), ret->size);

    return ret;

}

/**
 * @param ino
 * @param i null inode means empty inode
 */
void rocksdb_fs::write_inode(uint64_t ino, inode *i) {
    char key[20];
    sprintf(key, "%lu", ino);
    if(i == nullptr) {
        size_t size = 0;
        db->Put(WriteOptions(), key, Slice((char*)(&size), sizeof(size_t)));
    } else {
        size_t attr_size = sizeof(inode) - sizeof(uint8_t*);
        i->data = (uint8_t*)realloc(i->data, i->size + attr_size);
        memcpy(i->data + i->size, &i->size, attr_size);
        db->Put(WriteOptions(), key, Slice((char*)i->data, i->size + attr_size));
    }
}

void rocksdb_fs::drop_inode(uint64_t ino) {
    char key[20];
    sprintf(key, "%lu", ino);
    db->Delete(WriteOptions(), key);
}

/**
 * @return the last directory entry that can be retrieved
 * returning nullptr means that a directory has corrupted
 */
rfs_dentry* rocksdb_fs::lookup(char *path, bool& found) {

    auto dentry_ret = new rfs_dentry;

    // copy root directory entry to ret
    dentry_ret->ftype = dir;
    strcpy(dentry_ret->name, "/");
    dentry_ret->ino = ROOT_DENTRY_INO;
    dentry_ret->_inode = read_inode(ROOT_DENTRY_INO);
    found = true;

    // if path is root path
    if(path[0] == '/' && strlen(path) == 1) {
        return dentry_ret;
    }

    char* dir_name = strtok(path, "/");

    rfs_dentry_d* dentry_cursor;
    inode* i;
    int k;
    size_t dir_cnt;

    while(dir_name) {
        if(dentry_ret->ftype == dir) {
            dentry_cursor = (rfs_dentry_d*)(dentry_ret->_inode->data);
            dir_cnt = dentry_ret->_inode->size / sizeof(rfs_dentry_d);
            for(k = 0;k < dir_cnt;k++, dentry_cursor++) {
                if(strcmp(dentry_cursor->name, dir_name) == 0) {
                    break;
                }
            }
            if(k == dir_cnt) {
                RFS_DEBUG("rfs::lookup", "directory not found");
                found = false;
                break;
            }
        } else {
            // not a directory, still return the directory entry
            found = false;
            break;
        }

        dentry_ret->ftype = dentry_cursor->ftype;
        dentry_ret->ino = dentry_cursor->ino;
        strcpy(dentry_ret->name, dentry_cursor->name);
        delete []dentry_ret->_inode->data;
        delete dentry_ret->_inode;
        i = read_inode(dentry_ret->ino);
        // corrupted
        if(i == nullptr) {
            delete dentry_ret;
            return nullptr;
        }
        dentry_ret->_inode = i;

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
    size_t ori_size = parent->_inode->size;
    parent->_inode->size = ori_size + sizeof(rfs_dentry_d);
    parent->_inode->data = (uint8_t*)realloc(parent->_inode->data, parent->_inode->size);
    memcpy(parent->_inode->data + ori_size, dentry_d, sizeof(rfs_dentry_d));

    // write back to parent _inode
    write_inode(parent->ino, parent->_inode);
    // write back to new _inode
    write_inode(dentry_d->ino, nullptr);

}

void rocksdb_fs::drop_dentry_d(rfs_dentry *parent, rfs_dentry_d* dentry_d) {
    size_t pre_size = (uint8_t*)dentry_d - parent->_inode->data;
    size_t post_size = parent->_inode->size - pre_size - sizeof(rfs_dentry_d);
    parent->_inode->size = pre_size + post_size;
    auto temp_inode = new uint8_t[pre_size + post_size];
    memcpy(temp_inode, parent->_inode->data, pre_size);
    memcpy(temp_inode + pre_size, dentry_d + 1,post_size);

    drop_inode(dentry_d->ino);
    delete parent->_inode->data;
    parent->_inode->data = temp_inode;
    write_inode(parent->ino, parent->_inode);

}

/**
 * find directory entry by file name in a parent directory entry
 * @param parent
 * @param name
 * @return
 */
rfs_dentry_d* rocksdb_fs::find_dentry(rfs_dentry *parent, const char *name) {
    rfs_dentry_d* dentry_cursor = (rfs_dentry_d*)parent->_inode->data;
    int i = parent->_inode->size / sizeof(rfs_dentry_d);
    for(;i > 0;i--, dentry_cursor++) {
        if(strcmp(dentry_cursor->name, name) == 0) {
            break;
        }
    }

    if(i == 0) {
        return nullptr;
    }

    return dentry_cursor;
}

void rocksdb_fs::free_dentry(rfs_dentry *dentry) {
    delete dentry->_inode->data;
    delete dentry->_inode;
    delete dentry;
}

char* rocksdb_fs::parent_path(const char *path, int& div_idx) {
    // remove the filename
    div_idx = strlen(path);
    while(div_idx >= 0 && path[div_idx] != '/') div_idx--;
    if(div_idx == 0) div_idx = 1;
    char* ret = new char[div_idx + 1];
    memcpy(ret, path, div_idx + 1);
    ret[div_idx] = '\0';
    if(div_idx == 1) div_idx = 0;
    return ret;
}