#!/usr/bin/env python3

import os

from main.settings._utils import get_hdfs_uri_from_config


class Config(object):
    ROOT_DIR = os.getenv('DATAALGOS_HOME')
    if ROOT_DIR is None:
        from pathlib import Path
        ROOT_DIR = Path(__file__).parents[4]

    HDFS_USERNAME = os.environ.get('HDFS_USERNAME', 'hdfs')
    HDFS_URI = os.environ.get('HDFS_URI', get_hdfs_uri_from_config())

    @staticmethod
    def get_hdfs_basepath():
        return os.path.join(Config.HDFS_URI, 'user', Config.HDFS_USERNAME)
