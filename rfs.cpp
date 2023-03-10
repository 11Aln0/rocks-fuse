//
// Created by aln0 on 3/6/23.
//

#include<iostream>
#include "rocksdb_fs.h"

using namespace std;

struct fuse_options {
     const char *dbpath;
     int show_help;
} fuse_opts;

#define OPTION(t, p) {t, offsetof(fuse_options, p), 1}
static const fuse_opt option_spec[] = {
        OPTION("--dbpath=%s", dbpath),
        OPTION("--help", show_help),
        FUSE_OPT_END
};

static rocksdb_fs fs;

void* rfs_init(fuse_conn_info* conn_info, fuse_config *cfg) {
    int ret = fs.connect(fuse_opts.dbpath);
    if(ret != 0) goto err;

    ret = fs.mount();
    if(ret != 0) goto err;
    goto ok;

    err: fuse_exit(fuse_get_context()->fuse);
    ok: return nullptr;
}

void rfs_destroy(void* p) {
    fs.close();
    fuse_exit(fuse_get_context()->fuse);
}

int rfs_mkdir(const char* path, mode_t mode) {
    return fs.mkdir(path);
}

int rfs_getattr(const char* path, struct stat* stat, fuse_file_info *fi) {
    return fs.getattr(path, stat);
}

int rfs_readdir(const char* path, void* buf,fuse_fill_dir_t filter,
                off_t offset, struct fuse_file_info* fi, fuse_readdir_flags flags) {
    return fs.readdir(path, buf, filter);
}

int rfs_mknod(const char* path, mode_t mode, dev_t dev) {
    return fs.mknod(path, mode);
}

int rfs_read (const char* path, char* buf, size_t size, off_t offset, fuse_file_info * fi) {
    return fs.read(path, buf, size, offset);
}

int rfs_write(const char* path, const char* buf, size_t size, off_t offset, fuse_file_info* fi) {
    return fs.write(path, buf, size, offset);
}

int rfs_rmdir(const char* path) {
    return fs.rmdir(path);
}

void show_help() {
    printf("File-system specific options:\n"
           "    --dbpath=<s>        Path to save rocksdb's persistent file"
           "                        (default: \".//db\")\n"
           "\n");
}

static const fuse_operations rfs_oper = {
        .getattr = rfs_getattr,
        .mknod = rfs_mknod,
        .mkdir = rfs_mkdir,
        .rmdir = rfs_rmdir,
        .read = rfs_read,
        .write = rfs_write,
        .readdir = rfs_readdir,
        .init = rfs_init,
        .destroy = rfs_destroy,
};

int main(int argc, char *argv[])
{

    fuse_args args = FUSE_ARGS_INIT(argc, argv);

    fuse_opts.dbpath = strdup("./db");
    if(fuse_opt_parse(&args, &fuse_opts, option_spec, NULL) == -1) {
        return 1;
    }

    if(fuse_opts.show_help) {
        show_help();
        args.argv[0][0] = '\0';
    }

    int ret = fuse_main(args.argc, args.argv, &rfs_oper, NULL);
    fuse_opt_free_args(&args);

    return ret;
}