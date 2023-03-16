//
// Created by aln0 on 3/15/23.
//

#include "types.h"


/**
 * deserialize the data into inode
 * @param data byte data in db
 * @param size data_size
 */
inode_t::inode_t(void *data, size_t size) {
//    size_t attr_size = sizeof(inode_t) - sizeof(uint8_t*);
//    this->f_size = size + attr_size;
    this->d_size = size;
    this->_data = new uint8_t[size];
    memcpy(this->_data, data, size);
//    memcpy(&this->_data + sizeof(void*), (char*)data + this->d_size, attr_size);
}

const uint8_t *inode_t::data() const {
    return _data;
}

void inode_t::append_dentry_d(rfs_dentry_d *d) {
    // TODO may reserve one empty dentry's memory
    size_t ori_sz = this->d_size;
//    this->f_size += sizeof(rfs_dentry_d);
    this->d_size += sizeof(rfs_dentry_d);

    auto tmp_dt = new uint8_t[this->d_size];
    memcpy(tmp_dt, this->_data, ori_sz);
    memcpy(tmp_dt + ori_sz, d, sizeof(rfs_dentry_d));
//    memcpy(tmp_dt + this->d_size, this->_data + ori_sz, this->f_size - this->d_size);

    delete[] this->_data;
    this->_data = tmp_dt;

}

void inode_t::drop_dentry_d(rfs_dentry_d *d) {
//    size_t ori_sz = this->d_size;
    size_t pre_sz = (uint8_t*)d - this->_data;
    size_t post_sz = this->d_size - pre_sz - sizeof(rfs_dentry_d);
    this->d_size = pre_sz + post_sz;
//    this->f_size -= sizeof(rfs_dentry_d);

    auto tmp_dt = new uint8_t[this->d_size];
    memcpy(tmp_dt, this->_data, pre_sz);
    memcpy(tmp_dt + pre_sz, d + 1, post_sz);
//    memcpy(tmp_dt + this->d_size, this->_data + ori_sz, this->f_size - this->d_size);

    delete[] this->_data;
    this->_data = tmp_dt;
}

void inode_t::write_data(const char *buf, size_t size, off_t offset) {
    size_t n_sz = offset + size;
    if(n_sz <= this->d_size) {
        memcpy(this->_data, buf, size);
    } else {
//        size_t ori_sz = this->d_size;
//        this->f_size = (n_sz - this->d_size) + this->f_size;
//        this->d_size = n_sz;
        auto tmp_dt = new uint8_t[n_sz];
        memcpy(tmp_dt, this->_data, this->d_size);
        memcpy(tmp_dt + offset, buf, size);
//        memcpy(tmp_dt + offset + size, this->_data + ori_sz, this->f_size - this->d_size);
        delete[] this->_data;
        this->_data = tmp_dt;
        this->d_size = n_sz;
    }

}

inode_t::~inode_t() {
    delete[] _data;
}


