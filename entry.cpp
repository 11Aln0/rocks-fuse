//
// Created by aln0 on 3/6/23.
//

#include "rocksdb_fs.h"
#include <functional>

using std::bind;

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
    int ret = fs.mknod(path, mode);
    return ret < 0 ? ret : 0;
}

int rfs_read (const char* path, char* buf, size_t size, off_t offset, fuse_file_info * fi) {
    return fs.read(path, buf, size, offset, fi);
}

int rfs_write(const char* path, const char* buf, size_t size, off_t offset, fuse_file_info* fi) {
    return fs.write(path, buf, size, offset, fi);
}

int rfs_rmdir(const char* path) {
    return fs.rmdir(path);
}

int rfs_unlink(const char* path) {
    return fs.unlink(path);
}

int rfs_utimens(const char* path, const timespec tv[2], fuse_file_info* fi){
    return 0;
}

int rfs_fsync(const char* path, int, fuse_file_info* fi) {
    return fs.fsync(fi);
}

int rfs_open(const char* path, fuse_file_info* fi) {
    return fs.open(path, fi);
}

int rfs_create(const char* path, mode_t mode, fuse_file_info* fi) {
    return fs.create(path, mode, fi);
}

int rfs_release(const char* path, fuse_file_info* fi) {
    return fs.release(fi);
}

int rfs_truncate(const char* path, off_t size, struct fuse_file_info *fi) {
    return fs.truncate(path, size, fi);
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
        .unlink = rfs_unlink,
        .rmdir = rfs_rmdir,
        .truncate = rfs_truncate,
        .open = rfs_open,
        .read = rfs_read,
        .write = rfs_write,
        .release = rfs_release,
        .fsync = rfs_fsync,
        .readdir = rfs_readdir,
        .init = rfs_init,
        .destroy = rfs_destroy,
        .create = rfs_create,
        .utimens = rfs_utimens,
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