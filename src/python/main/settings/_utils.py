#!/usr/bin/env python3
"""Helper functions for configuration."""

import os
import xml.etree.ElementTree as ET


def get_hdfs_uri_from_config():
    """Get HDFS URI from core-site.xml."""
    path = os.path.join(os.environ.get('HADOOP_HOME', '/usr/local/hadoop'), 'etc', 'hadoop', 'core-site.xml')
    tree = ET.parse(path)
    root = tree.getroot()

    tags = root.findall('property')
    for tag in tags:
        if tag.find('name').text == 'fs.defaultFS':
            return tag.find('value').text

    return 'hdfs://localhost:8020'
