#!/usr/bin/env python3
"""Load file processed by Hadoop MapReduce and process by pyspark.

It is assumed that all data is properly processed and
invalid data is filtered out at hadoop mapreduce step.

In this app, calculate similarity between each movie pair.
Similarity metrics used here are:
    - Cosine similarity
    - Pearson correlation coefficient
    - Jaccard similarity coefficient
"""

import os
import logging
import argparse

import math
from datetime import datetime
from decimal import Decimal
from typing import List, Iterable

import pyspark
from pyspark import SparkContext, SparkConf

from main.settings import Config


log = logging.getLogger(__name__)

sqrt = math.sqrt


def normalize_pair_data(data1, data2, maxval):
    """Standadize given two data for calculating similarity.
    Adjust data range in [0.0, 1.0]
    """
    return data1 / maxval, data2 / maxval


def compute_cosine_similarity(rate1, rate2, ndata1, ndata2):
    """Compute cosine similarity
        (https://en.wikipedia.org/wiki/Cosine_similarity)

    Args:
        rate1, rate2, ndata1, ndata2: decimal.Decimal

    Returns:
        decimal.Decimal
    """

    return (((rate1 * ndata1) + (rate2 * ndata2)) \
        / ((rate1 ** 2 + rate2 ** 2) * (ndata1 ** 2 + ndata2 ** 2))) \
        .sqrt() \
        .quantize(Decimal('0.0001'))


def compute_pearson(rate1, rate2, ndata1, ndata2):
    """Compute Pearson correlation coefficient
        (https://en.wikipedia.org/wiki/Pearson_correlation_coefficient)
    """
    return Decimal(0.0)


def compute_jaccard(rates1, rates2):
    """Compute Jaccard similarity coefficient
        (https://en.wikipedia.org/wiki/Jaccard_index)

    This is comparing rates by each users.
    The inputs can be very sparse.
    """
    return Decimal(0.0)


def split_row_from_movie_data(row: str) -> List:
    # input row => 'user_id,rating,date'
    row = row.split(',')
    # convert into int
    # timestamp will be divided by (60 * 60) to ignore sec. and min.
    row = [int(row[0]), int(row[1]), int(datetime(*map(int, row[2].split('-'))).timestamp()) // 3600]
    return row


def comp_days(date1: int, date2: int) -> int:
    """Compare two dates prepresented as intger and return the days of the difference."""
    return (date1 - date2) // 24


def flatten_user_data(data: pyspark.rdd.RDD) -> pyspark.rdd.RDD:
    # convert and flatten user data
    #   [('{movie_id}', '{user_id},{rating},{date}\n...')]
    #   => [[{movie_id}, [{user_id},{rating},{timestamp}], ... ]
    return data \
        .map(lambda x: (int(x[0].split('-')[0]), x[1].splitlines())) \
        .flatMap(lambda x: [[x[0], split_row_from_movie_data(item)] for item in x[1]])


def count_value_items(data: pyspark.rdd.RDD,
                      kv_sep: str='\t',
                      val_sep: str=',') -> pyspark.rdd.RDD:
    # split row into (key, value) pairs by kv_sep
    # then, count items in value split by val_sep
    return data \
        .map(lambda x: x.split(kv_sep)) \
        .map(lambda x: (int(x[0]), len(x[1].split(val_sep))))


def add_num_rates_to_user_data(users: pyspark.rdd.RDD,
                               movies: pyspark.rdd.RDD) -> pyspark.rdd.RDD:
    # swaping key and make joinable, then swap back to original shape
    return users \
        .map(lambda x: (x[1][0], (x[0], *x[1][1:]))) \
        .join(movies) \
        .map(lambda x: (x[1][0][0], (x[0], *x[1][0][1:], x[1][1])))


def build_movie_pairs(data: pyspark.rdd.RDD) -> pyspark.rdd.RDD:
    # join the same group to make combinatin
    # and only take the first movie id is smaller than the second one
    # to remove duplicates
    return data \
        .join(data) \
        .filter(lambda x: x[1][0][0] < x[1][1][0])


def calc_pair_parameters(users: pyspark.rdd.RDD,
                         max_raters: int) -> pyspark.rdd.RDD:
    # Key:   (movie1, movie2)
    # Value: (rating1, num_rate1, rating2, num_rate2, rating1*rating2)
    return users \
        .map(lambda x: (
                (x[1][0][0], x[1][1][0]),
                (Decimal(x[1][0][1]) / 5,
                 Decimal(x[1][0][3]) / max_raters,
                 Decimal(x[1][1][1]) / 5,
                 Decimal(x[1][1][3]) / max_raters)
            )
        )


def calc_similarity(data: pyspark.rdd.RDD) -> pyspark.rdd.RDD:
    # Calculate consine, Pearson, Jacarrd similarities
    return data \
        .reduceByKey(lambda a, b: [a_ + b_ for a_, b_ in zip(a, b)]) \
        .map(lambda x: (
            x[0],
            (
                compute_cosine_similarity(*x[1]),
                #compute_pearson(*x[1]),
                #compute_jaccard(*x[1])
            )
        ))


def combine_labels(items: Iterable, labels: Iterable) -> Iterable:
    if labels is None: return map(str, items)

    return [f'{label}={item}' for item, label in zip(items, labels)]


def convert_to_writable_format(items: Iterable, labels: Iterable=None) -> str:
    """Convert items stored in iterable to string to apply Writable."""
    if labels is not None:
        if len(items) != len(labels):
            log.warn(f'given label size of items does not match: {len(items)} != {len(labels)}')
            labels = None

    items = combine_labels(items, labels)
    return ','.join(items)


def main(sc: SparkContext,
         input_path: str,
         output_path: str,
         is_parquet: bool=False,
         from_local: bool=False,
         to_hdfs: bool=False,
         is_sequence: bool=False,
         is_gzip: bool=False) -> None:


    if not from_local:
        input_path = os.path.join(Config.get_hdfs_basepath(), input_path)

    # Stage 1: Read user dataset
    #   step 1: read files from hdfs
    user_path = os.path.join(input_path, 'users/*-r-00000')
    users = sc.wholeTextFiles(user_path) \
        .map(lambda x: (os.path.basename(x[0]), x[1])) \
        .filter(lambda x: x[0][0].isdigit())


    #   step 2: flatten data with user_id
    #       format ('{movie_id}-r-0000', 'id,rating,date\n...')
    #           -> (movie_id, [id, rating, date])
    users = flatten_user_data(users)

    # Stage 2: Read movie data
    movies = sc.textFile(os.path.join(input_path, 'movies/*'))


    # Stage 3: Add number of rates to user data
    #   step 1: calculate number of rates in the movie
    rates = count_value_items(movies)
    # get max num of rates used for normalization
    max_raters = rates \
        .map(lambda x: x[1]) \
        .max()

    #   step 2: Add number of rates to user data
    users = add_num_rates_to_user_data(users, rates)


    # Stage 3: Build all combinations of movie pairs per user
    users = build_movie_pairs(users)


    # Stage 4: Generate all movie combinations with necessary data
    #   format: [(movie1, movie2),
    #            (movie1_rate, movie2_rate, num_rates1, num_rates2)]
    movie_pairs = calc_pair_parameters(users, max_raters=max_raters)


    # Stage 5: Calculate correlations
    #   format: [movie1, movie2, cosine]
    # TODO: add Pearson and Jaccard
    movie_pairs = calc_similarity(movie_pairs)


    # Stage 6: Output the scores to a file in HDFS/Parquet
    if to_hdfs:
        output_path = os.path.join(Config.get_hdfs_basepath(), output_path)

    if is_parquet:
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType
        from pyspark.sql.types import StructField
        from pyspark.sql.types import LongType, FloatType

        # TODO: add Pearson and Jaccard
        schema = StructType([
            StructField('Movie1', LongType(), False),
            StructField('Movie2', LongType(), False),
            StructField('Cosine', FloatType(), True),
            #StructField('Pearson', FloatType(), True),
            #StructField('Jaccard', FloatType(), True)
        ])

        session = SparkSession.builder.config(conf=sc.getConf()) \
            .getOrCreate()

        data = movie_pairs \
            .map(lambda x: [*x[0], *map(float, x[1])])

        output_path = os.path.join(output_path, 'result.parquet')
        session.createDataFrame(data, schema=schema) \
            .write \
            .mode('overwrite') \
            .parquet(output_path)

    else:
        # write as simple text with Writable from Hadoop API
        labels = ['Cosine']

        if is_gzip:
            compClass = 'org.apache.hadoop.io.compress.GzipCodec'
        else:
            compClass = None

        data = movie_pairs.map(
                lambda x: (
                    convert_to_writable_format(x[0]),
                    convert_to_writable_format(x[1], labels)))
        if to_hdfs:
            if is_sequence:
                data.saveAsNewAPIHadoopFile(
                    output_path,
                    outputFormatClass='org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat',
                    keyClass='org.apache.hadoop.io.Text',
                    valueClass='org.apache.hadoop.io.Text'
                )
            else:
                data.saveAsTextFile(output_path, compressionCodecClass=compClass)

        else:
            data.saveAsTextFile(output_path, compressionCodecClass=compClass)
    log.info(f'Saved at: {output_path}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description='',
            formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-i', '--input', type=str,
                        required=True,
                        help='Input directory path containing dataset')
    parser.add_argument('-o', '--output', type=str,
                        required=True,
                        help='Output directory path')

    parser.add_argument('--parquet', action='store_true',
                        help='Set this to save as Parquet file')
    parser.add_argument('--from-local', action='store_true',
                        help='Set this to read data in local file system')
    parser.add_argument('--to-hdfs', action='store_true',
                        help='Set this to save data in hdfs')
    parser.add_argument('--sequenceFile', action='store_true',
                        help='Set this to save as SequenceFile\n'
                             'This works only when to_hdfs is set')
    parser.add_argument('--gzip', action='store_true',
                        help='Set this to compress data by gzip')
    args = parser.parse_args()

    input_path = args.input
    output_path = args.output
    is_parquet = args.parquet
    from_local = args.from_local
    to_hdfs = args.to_hdfs
    is_sequence = args.sequenceFile
    is_gzip = args.gzip

    conf = SparkConf() \
        .setAppName('movie-recommendation') \
        .setMaster('local[*]')

    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')

    main(sc,
         input_path,
         output_path,
         is_parquet,
         from_local,
         to_hdfs,
         is_sequence,
         is_gzip)
