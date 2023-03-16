//
// Created by aln0 on 3/7/23.
//

#include <memory>

#include "rocksdb_fs.h"


/**
 * @return  return nullptr means that an inode has corrupted
 */
inode_t* rocksdb_fs::read_inode(uint64_t ino) {
    string rV;
    char key[20];
    sprintf(key, "%lu", ino);
    Status s = db->Get(ReadOptions(), key, &rV);
    if(!s.ok()) {
        RFS_DEBUG("rfs::read_inode", "retrieve inode failed!");
        return nullptr;
    }

    return new inode_t(rV.data(), rV.size());

}

/**
 * @param ino
 * @param inode null inode_t means empty inode_t
 */
void rocksdb_fs::write_inode(uint64_t ino, const inode_t *inode) {
    char key[20];
    sprintf(key, "%lu", ino);
    if(inode == nullptr) {
        db->Put(WriteOptions(), key, Slice());
    } else {
        db->Put(WriteOptions(), key, Slice((char*)inode->data(), inode->d_size));
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
unique_ptr<rfs_dentry> rocksdb_fs::lookup(char *path, bool& found) {

    auto dentry_ret = make_unique<rfs_dentry>();

    // copy root directory entry to ret
    dentry_ret->ftype = dir;
    strcpy(dentry_ret->name, "/");
    dentry_ret->ino = ROOT_DENTRY_INO;
    dentry_ret->inode = unique_ptr<inode_t>(read_inode(dentry_ret->ino));
    found = true;

    // if path is root path
    if(path[0] == '/' && strlen(path) == 1) {
        return dentry_ret;
    }

    char* dir_name = strtok(path, "/");

    const rfs_dentry_d* dentry_cursor;
    int k;
    size_t dir_cnt;

    while(dir_name) {
        if(dentry_ret->ftype == dir) {
            dentry_cursor = (const rfs_dentry_d*)(dentry_ret->inode->data());
            dir_cnt = dentry_ret->inode->d_size / sizeof(rfs_dentry_d);
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
        dentry_ret->inode = unique_ptr<inode_t>(read_inode(dentry_ret->ino));
        // corrupted
        if(dentry_ret->inode == nullptr) {
            return nullptr;
        }

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
    parent->inode->append_dentry_d(dentry_d);

    // write back to parent inode
    write_inode(parent->ino, parent->inode.get());
    // write back to new inode
    write_inode(dentry_d->ino, nullptr);

}

void rocksdb_fs::drop_dentry_d(rfs_dentry *parent, rfs_dentry_d* dentry_d) {
    parent->inode->drop_dentry_d(dentry_d);
    drop_inode(dentry_d->ino);
    write_inode(parent->ino, parent->inode.get());
}

/**
 * find directory entry by file name in a parent directory entry
 * @param parent
 * @param name
 * @return
 */
rfs_dentry_d* rocksdb_fs::find_dentry(rfs_dentry *parent, const char *name) {
    rfs_dentry_d* dentry_cursor = (rfs_dentry_d*)parent->inode->data();
    int i = parent->inode->d_size / sizeof(rfs_dentry_d);
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