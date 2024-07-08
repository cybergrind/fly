#!/usr/bin/env python3
import argparse
import errno
import logging
import multiprocessing
import os
import re
import stat
import struct
import tempfile
import time
from pathlib import Path

import fuse


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
log = logging.getLogger('fly')
fuse.fuse_python_api = (0, 2)
TIME_PAT = re.compile(r'.*\/\d+\.\d+')
# num files, array[name_length, name]
HEADER_STRUCT = struct.Struct('I')

if not hasattr(fuse, '__version__'):
    raise RuntimeError("your fuse-py doesn't know of fuse.__version__, probably it's too old.")


def parse_args():
    parser = argparse.ArgumentParser(description='DESCRIPTION')
    parser.add_argument('fname', type=Path)
    parser.add_argument('mountpoint', nargs='?', default='/tmp/aaa', type=Path)
    parser.add_argument('--ttl', type=int, default=3)
    return parser.parse_args()


def call_fuse_exit(mountpoint):
    # start with nohup
    multiprocessing.Process(target=auto_unmount, args=(mountpoint,)).start()


hello_path = '/hello'
hello_str = b'Hello World!\n'


class MyStat(fuse.Stat):
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0


class FileWrapper:
    """
    know how to write to the arbitrary parts of the file
    """

    def __init__(self, path: Path):
        self.path = path
        if not path.exists():
            path.touch()

        self.inner_files = set()

    def remove_data(self, offset, size):
        """
        remove data in file and free space
        """
        if offset + size > self.path.stat().st_size:
            raise ValueError('offset + size > file size')
        temp = tempfile.NamedTemporaryFile(delete=False)
        try:
            with self.path.open('rb') as f:
                f.seek(0, os.SEEK_SET)
                temp.write(f.read(offset))
                f.seek(offset + size, os.SEEK_SET)
                temp.write(f.read())
            temp.seek(0)
            self.path.write_bytes(temp.read())
        finally:
            os.unlink(temp.name)

    def write(self, offset, buff):
        if offset > self.path.stat().st_size:
            # increase size
            self.path.write_bytes(b'\0' * (offset - self.path.stat().st_size))
        with self.path.open('r+b') as f:
            f.seek(offset)
            f.write(buff)


class Fly(fuse.Fuse):
    def add_args(self, args):
        self._ctime = time.time()
        self._args = args
        self.dst = args.fname
        self.mountpoint = args.mountpoint

    def getattr(self, path):
        if time.time() - self._ctime > self._args.ttl:
            call_fuse_exit(self.mountpoint)
            return -errno.ENOENT

        st = MyStat()
        st.st_ctime = st.st_mtime = st.st_atime = int(time.time())

        if path == '/':
            st.st_mode = stat.S_IFDIR | 0o755
            st.st_nlink = 2
        elif path == hello_path:
            st.st_mode = stat.S_IFREG | 0o444
            st.st_nlink = 1
            st.st_size = len(hello_str)
        elif TIME_PAT.match(path):
            st.st_mode = stat.S_IFREG | 0o444
            st.st_nlink = 1
            st.st_size = 0
        else:
            return -errno.ENOENT
        return st

    def readdir(self, path, offset):
        for r in ('.', '..', hello_path[1:], str(time.time())):
            yield fuse.Direntry(r)

    def open(self, path, flags):
        if path != hello_path or TIME_PAT.match(path):
            return -errno.ENOENT
        accmode = os.O_RDONLY | os.O_WRONLY | os.O_RDWR
        if (flags & accmode) != os.O_RDONLY:
            return -errno.EACCES

    def write(self, path, buf, offset):
        if path != hello_path:
            return -errno.ENOENT
        return len(buf)

    def read(self, path, size, offset):
        if path != hello_path:
            return -errno.ENOENT
        slen = len(hello_str)
        if offset < slen:
            if offset + size > slen:
                size = slen - offset
            buf = hello_str[offset : offset + size]
        else:
            buf = b''
        return buf


def auto_unmount(mountpoint):
    """
    wait 10 sec and unmount
    """
    time.sleep(0.01)
    os.system(f'fusermount -u {mountpoint}')


def mount(args):
    f = Fly(
        version='%prog ' + fuse.__version__,
        usage='%(prog)s [options] <mountpoint>',
        dash_s_do='setsingle',
    )
    f.add_args(args)
    f.parser.add_option(mountopt=args.mountpoint, metavar='PATH', default=args.mountpoint)
    f.main(['fly.py', str(args.mountpoint)])


def main():
    args = parse_args()

    if not args.fname.exists():
        log.error('File %s does not exist', args.fname)
        exit(1)
    mount(args)


if __name__ == '__main__':
    main()
