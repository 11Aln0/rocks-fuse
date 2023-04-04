//
// Created by aln0 on 3/15/23.
//

#include "types.h"
#include <cstddef>

/**
 * |------------------size-----------------|
 * |---------data_size----------||-attr_sz-|
 * |--used_dat_sz--||---nused---||-attr_sz-|
 */

/**
 * empty inode
 */
inode_t::inode_t() {
    this->used_dat_sz = 0;
    this->attr_sz = sizeof(inode_t) - offsetof(inode_t, used_dat_sz) - sizeof(size_t);
    this->size = this->attr_sz;
    this->_data = new uint8_t[this->size + this->attr_sz];
}

/**
 * deserialize the data into inode
 * @param data byte data in db
 * @param size the size of the whole inode
 */
inode_t::inode_t(const char* data, size_t size) {
    this->size = size;
    // currently all attributes above used_dat_sz and used_dat_sz itself will not be persistent
    this->attr_sz = sizeof(inode_t) - offsetof(inode_t, used_dat_sz) - sizeof(size_t);
    this->used_dat_sz = size - attr_sz;
    this->_data = new uint8_t[this->size];

    memcpy(this->_data, data, this->used_dat_sz);
    memcpy(&this->used_dat_sz + 1, data + this->used_dat_sz, this->attr_sz); // copy attributes
}

const uint8_t *inode_t::data() const {
    return _data;
}

/**
 * adjust the attributes to data before write back to db
 */
void inode_t::before_write_back() {
    memcpy(this->_data + this->used_dat_sz, &this->used_dat_sz + 1, this->attr_sz);
}

/**
 * append one dentry to the end of data
 */
void inode_t::append_dentry_d(rfs_dentry_d *d) {
    size_t dat_sz = this->size - this->attr_sz;
    if(this->used_dat_sz + sizeof(rfs_dentry_d) > dat_sz) {
        this->size = this->used_dat_sz + sizeof(rfs_dentry_d) + this->attr_sz;
        auto tmp_dt = new uint8_t[this->size];
        memcpy(tmp_dt, this->_data, this->used_dat_sz);
        memcpy(tmp_dt + this->used_dat_sz, d, sizeof(rfs_dentry_d));

        delete[] this->_data;
        this->_data = tmp_dt;
    } else {
        memcpy(this->_data + this->used_dat_sz, d, sizeof(rfs_dentry_d));
    }

    this->used_dat_sz += sizeof(rfs_dentry_d);
}

/**
 * drop dentry of a directory's inode
 */
void inode_t::drop_dentry_d(rfs_dentry_d *d) {
    size_t dir_cnt = this->used_dat_sz / sizeof(rfs_dentry_d);
    size_t post_cnt = (dir_cnt - 1 - (d - (rfs_dentry_d*) this->_data));

    d++;
    for(size_t i = 0;i < post_cnt;i++, d++) {
        memcpy(d - 1, d, sizeof(rfs_dentry_d));
    }

    this->used_dat_sz -= sizeof(rfs_dentry_d);
}

/**
 * overwrite the dst dentry of inode with src
 */
void inode_t::overwrite_dentry_d(rfs_dentry_d *src, rfs_dentry_d* dst) {
    memcpy(dst, src, sizeof(rfs_dentry_d));
}

void inode_t::write_data(const char *buf, size_t size, off_t offset) {
    size_t new_dat_sz = offset + size;
    size_t dat_sz = this->size - this->attr_sz;
    if(new_dat_sz <= dat_sz) {
        memcpy(this->_data, buf, size);
        if(new_dat_sz > this->used_dat_sz) {
            this->used_dat_sz = new_dat_sz;
        }
    } else {
        this->size = new_dat_sz + this->attr_sz;
        auto tmp_dt = new uint8_t[this->size];
        memcpy(tmp_dt, this->_data, dat_sz);
        memcpy(tmp_dt + offset, buf, size);

        delete[] this->_data;
        this->_data = tmp_dt;
        this->used_dat_sz = new_dat_sz;
    }


}

void inode_t::truncate(size_t size) {
    size_t dat_sz = this->size - this->attr_sz;
    if(size > dat_sz) {
        // need to adjust the data
        this->size = size + this->attr_sz;
        auto tmp_dt = new uint8_t[this->size];
        memcpy(tmp_dt, this->_data, this->used_dat_sz);

        delete[] this->_data;
        this->_data = tmp_dt;
    } else {
        if(size < this->used_dat_sz) {
            this->used_dat_sz = size;
        }
    }
}

inode_t::~inode_t() {
    delete[] _data;
}


