#!/usr/bin/env python3

import os


class Config(object):
    ROOT_DIR = os.getenv('DATAALGOS_HOME')
    if ROOT_DIR is None:
        from pathlib import Path
        ROOT_DIR = Path(__file__).parents[4]

    HDFS_URI = os.environ.get('HDFS_URI', 'hdfs://localhost:9000')
