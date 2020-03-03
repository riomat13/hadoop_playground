#!/usr/bin/env python3
"""Load file processed by Hadoop MapReduce and process by pyspark.

It is assumed that all data is properly processed and
invalid data is filtered out at hadoop mapreduce step.

In this app, calculate similarity between each movie pair.
Similarity metrics used here are:
    - Cosine similarity
        - By mean of rates and number of raters
        - By each rate comparison
    - Pearson correlation coefficient (not implemented yet)
    - Jaccard similarity coefficient
        - By rated or not (binary comparison)
        - By exactly matching rate value (it has to be smaller than above one)
            example:
                A = {1: 3, 2: 5, 4: 1, 7: 3}
                B = {2: 4, 4: 1, 6: 2, 7: 3}
                jaccard_score(A, B) = 2 (id=4,7) / 5 (id=1,2,4,6,7) = 0.4
"""

import os
import logging
import argparse

import math
from datetime import datetime
from decimal import Decimal
from itertools import combinations
from typing import List, Iterable

import numpy as np

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.ml.linalg import SparseVector

from main.settings import Config


log = logging.getLogger(__name__)

sqrt = math.sqrt


def convert_to_sparse_vector(data: dict, nmax: int) -> SparseVector:
    """Convert array into sparse vector."""
    # arg in sparse vector is 0-indexed
    return SparseVector(nmax+1, {k: v for k, v in data.items()})


def normalize_pair_data(data1, data2, maxval):
    """Standadize given two data for calculating similarity.
    Adjust data range in [0.0, 1.0]
    """
    return data1 / maxval, data2 / maxval


def cosine_similarity_with_mean_rates(rate1, rate2, ndata1, ndata2):
    """Compute cosine similarity
        (https://en.wikipedia.org/wiki/Cosine_similarity)

    This is for simplification and may not represent similarity well
    since compare by mean value.

    Args:
        rate1, rate2, ndata1, ndata2: decimal.Decimal

    Returns:
        decimal.Decimal
    """
    return (((rate1 * ndata1) + (rate2 * ndata2)) \
        / ((rate1 ** 2 + rate2 ** 2) * (ndata1 ** 2 + ndata2 ** 2))) \
        .sqrt() \
        .quantize(Decimal('0.0001'))


def cosine_similarity(rates1: SparseVector, rates2: SparseVector) -> Decimal:
    """Compute cosine similarity
        (https://en.wikipedia.org/wiki/Cosine_similarity)

    Args:
        rate1, rate2: pyspark.ml.linalg.SparseVector
            rate values by each user

    Returns:
        decimal.Decimal
    """
    dot_rates = Decimal(rates1.dot(rates2))
    sq_rates1 = Decimal(np.sum(np.square(rates1.values)))
    sq_rates2 = Decimal(np.sum(np.square(rates2.values)))

    return dot_rates / (np.sqrt(sq_rates1) * np.sqrt(sq_rates2))


def compute_pearson(rate1, rate2, ndata1, ndata2):
    """Compute Pearson correlation coefficient
        (https://en.wikipedia.org/wiki/Pearson_correlation_coefficient)
    """
    # TODO
    return Decimal(0.0)


def jaccard_score_binary(rates1: SparseVector, rates2: SparseVector) -> Decimal:
    """Compute Jaccard similarity coefficient
        (https://en.wikipedia.org/wiki/Jaccard_index)

    This is comparing rates by each users.

    This will ignore the actual rate, and this assumes people who watch
    the same movies have similar preference, therefore those
    movies are similar.
    """
    # is there efficient way to handle sparse vector?
    r1 = rates1.toArray()
    r2 = rates2.toArray()
    union = int(sum(((r1 > 0) == (r2 > 0)) * (r1 > 0)))
    intersection = int(sum((r1 + r2) > 0))
    return Decimal(union) / intersection


def jaccard_score(rates1: SparseVector, rates2: SparseVector) -> Decimal:
    """Compute Jaccard similarity coefficient
        (https://en.wikipedia.org/wiki/Jaccard_index)

    This is comparing rates by each users.

    This will consider the rate value as well s.t. even if bothe are rated,
    it is not regarded as the same if the rate values are not the same.
    """
    r1 = rates1.toArray()
    r2 = rates2.toArray()
    union = int(sum((r1 == r2) * (r1 > 0)))
    intersection = int(sum((r1 + r2) > 0))
    return Decimal(union) / intersection


def split_row_from_movie_data(row: str) -> List:
    # input row => 'user_id,rating,date'
    row = row.split(',')
    # convert into int
    # timestamp will be divided by (60 * 60) to ignore sec. and min.
    row = (int(row[0]), int(row[1]), int(datetime(*map(int, row[2].split('-'))).timestamp()) // 3600)
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
        .flatMap(lambda x: [(x[0], split_row_from_movie_data(item)) for item in x[1]])


def flatten_movie_data(data: pyspark.rdd.RDD) -> pyspark.rdd.RDD:
    # convert and flatten user data
    #   [('{movie_id}', '{user_id1},{user_id2},...')]
    #   => [({movie_id}, ({user_id},{num_of_raters})), ... ]
    return data \
        .map(lambda x: x.split('\t')) \
        .map(lambda x: (int(x[0]), tuple(map(int, x[1].split(','))))) \
        .map(lambda x: ((x[0], len(x[1])), x[1])) \
        .flatMapValues(lambda x: x) \
        .map(lambda x: (x[0][0], (x[1], x[0][1])))


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


def build_sparse_vector_with_ratings(users: pyspark.rdd.RDD,
                                     max_user_id: int) -> pyspark.rdd.RDD:
    """Apply join user data to movie data.
    
    Input format:
        users:  (user_id, (movie_id, rating, date))
    Output format:
        (movie_id, (SparseVector(user_id, rating)))
    """
    return users \
        .map(lambda x: (x[1][0], (x[0], x[1][1]))) \
        .groupByKey() \
        .mapValues(list) \
        .map(lambda x: (x[0], {items[0]:items[1] for items in x[1]})) \
        .map(lambda x: (x[0], convert_to_sparse_vector(x[1], max_user_id)))


def build_item_pairs_by_key(data: pyspark.rdd.RDD) -> pyspark.rdd.RDD:
    """Build combination of items by key.
    Input format:
        (id, item)
    Output format:
        ((id1, id2), (item1, item2))
    """
    # join the same group to make combinatin
    # and only take the first id is smaller than the second one
    # to remove duplicates
    return data \
        .cartesian(data) \
        .map(lambda x: ((x[0][0], x[1][0]), (x[0][1], x[1][1]))) \
        .filter(lambda x: x[0][0] < x[0][1])


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
        .map(lambda x: (
            x[0],
            (
                cosine_similarity(*x[1]),
                #pearson(*x[1]),
                jaccard_score_binary(*x[1]),
                jaccard_score(*x[1])
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

    log.info(f'Loading data from: {input_path}')

    # Stage 1: Read user dataset
    #   step 1: read files from hdfs
    user_path = os.path.join(input_path, 'users/*-r-00000')
    users = sc.wholeTextFiles(user_path) \
        .map(lambda x: (os.path.basename(x[0]), x[1])) \
        .filter(lambda x: x[0][0].isdigit())


    #   step 2: flatten data with user_id
    #       format ('{user_id}-r-0000', 'movie_id,rating,date\n...')
    #           -> (user_id, [movie_id, rating, date])
    users = flatten_user_data(users)
    # use for sparse vector
    max_user_id = users \
        .max(key=lambda x: x[0])[0]


    # Stage 2: Read movie data and flatten
    #movies = sc.textFile(os.path.join(input_path, 'movies/*'))
    #movies = flatten_movie_data(movies)


    # Stage 2: Build sparse vector of ratings
    movies = build_sparse_vector_with_ratings(users, max_user_id)


    # Stage 3: Build all combinations of movie pairs per user
    #   format: [(movie1, movie2),
    #            (SparseVector(movie1_rate), SparseVector(movie2_rate), num_rates1, num_rates2)]
    movie_pairs = build_item_pairs_by_key(movies)


    # Stage 4: Calculate correlations
    #   format: [movie1, movie2, cosine]
    # TODO: add Pearson
    output = calc_similarity(movie_pairs)


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
            StructField('Cosine-mean', FloatType(), True),
            StructField('Cosine', FloatType(), True),
            #StructField('Pearson', FloatType(), True),
            StructField('Jaccard-bin', FloatType(), True),
            StructField('Jaccard', FloatType(), True)
        ])

        session = SparkSession.builder.config(conf=sc.getConf()) \
            .getOrCreate()

        output = output \
            .map(lambda x: [*x[0], *map(float, x[1])])

        output_path = os.path.join(output_path, 'result.parquet')
        session.createDataFrame(output, schema=schema) \
            .write \
            .mode('overwrite') \
            .parquet(output_path)

    else:
        # write as simple text with Writable from Hadoop API
        labels = ['Cosine', 'Jaccard-bin', 'Jaccard']

        if is_gzip:
            compClass = 'org.apache.hadoop.io.compress.GzipCodec'
        else:
            compClass = None

        output = output.map(
                lambda x: (
                    convert_to_writable_format(x[0]),
                    convert_to_writable_format(x[1], labels)))
        if to_hdfs:
            if is_sequence:
                output.saveAsNewAPIHadoopFile(
                    output_path,
                    outputFormatClass='org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat',
                    keyClass='org.apache.hadoop.io.Text',
                    valueClass='org.apache.hadoop.io.Text'
                )
            else:
                output.saveAsTextFile(output_path, compressionCodecClass=compClass)

        else:
            output.saveAsTextFile(output_path, compressionCodecClass=compClass)
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
