#!/usr/bin/env python3

import os
import re
from typing import List, Iterator

from pyspark import SparkContext

from main.settings import Config


def get_filenames(sc: SparkContext, path: str, abspath: bool=True, pattern: str=None) -> List[str]:
    """Get file names from HDFS with Java API.

    Args:
        sc: SparkContext
        path: str
            directory path which contains files in HDFS
        abspath: bool
            set to True if returns absolute path,
            set to False if only nees base file names
        pattern: str
            filter out files which does not match the pattern by regix

    Return:
        list(str): list of file names
    """
    # import Java API
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem

    fs = FileSystem.get(URI(Config.HDFS_URI), sc._jsc.hadoopConfiguration())
    list_status = fs.listStatus(Path(path))

    files = []
    for f in list_status:
        files.append(f.getPath())

    if abspath:
        files = [f.toString() for f in files]
    else:
        files = [f.getName() for f in files]

    # filter out if file name does not match the given pattern
    # this is used to filter out files such as `_SUCCESS`
    if pattern is not None:
        pattern = re.compile(pattern)

        files = [f for f in files if pattern.match(f)]

    return files


def build_paths(path_to_dir: str, filenames: List[str]) -> Iterator[str]:
    """Build filepaths from filenames stored in HDFS
    which data is processed by Hadoop MapReduce.

    This function assumes the files are named `{filename}-r-00000`

    Args:
        path_to_dir: str
            path to directory to set path
            this must include root such as:
                path_to_dir='/user/hdfs/path/to/dir'
        filenames: list(str)
            a list of file names to build absolute paths in hdfs

    Returns:
        iterator of str:
            each path will be `{HDFS_URI}/{path_to_dir}/{filename}`
            for instance:
                'hdfs://localhost:8020/user/hdfs/path/to/dir/file1.txt'
    """
    for filename in filenames:
        filename = filename.split('-')[0]
        yield os.path.join(Config.HDFS_URI, path_to_dir, filename)
