#!/usr/bin/env python3

import os.path
import logging

from main.settings import Config

log = logging.getLogger(__file__)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Spark operation for processing Border Crossing Entry data')
    parser.add_argument('-i', '--input', type=str,
                        default='input',
                        help='input directory path. if not specified, load from input/*')

    args = parser.parse_args()
    in_dir = os.path.join(Config.ROOT_DIR, args.input)
    out_dir = os.path.join(Config.ROOT_DIR, 'output', 'border', 'spark')
    log.info(f'Input directory path: {in_dir}')
    log.info(f'Output directory path: {out_dir}')

    from pyspark.sql import SparkSession, Window
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    import pyspark.sql.functions as F

    spark = SparkSession.builder \
        .master('local') \
        .appName('Border Crossing Entry') \
        .getOrCreate()

    # set up schema for target data
    schema = StructType([
        StructField('Port Name', StringType(), False),
        StructField('State', StringType(), False),
        StructField('Port Code', StringType(), False),
        StructField('Border', StringType(), False),
        StructField('Date', StringType(), False),
        StructField('Measure', StringType(), False),
        StructField('Value', IntegerType(), False)
    ])

    # Stage 1:
    #   load to DataFrame and drop unsed columns
    #   then group by keys and sort by the key
    dttm_format = 'MM/dd/yyyy hh:mm:ss a'

    df = spark.read.format('csv') \
            .option('header', 'false') \
            .schema(schema) \
            .load(os.path.join(in_dir, '*')) \
            .select('Border', 'Date', 'Measure', 'Value') \
            .groupBy('Date', 'Border', 'Measure') \
            .agg(F.sum('Value').alias('Value')) \
            .withColumn('Date', F.from_unixtime(F.unix_timestamp('Date', dttm_format),
                                                'yyyy-MM-dd HH:mm:ss').cast('timestamp')) \
            .orderBy('Date', 'Border', 'Measure')

    # Stage 2:
    #   calculate cumulative sum upto previous month
    window = Window.orderBy('Date') \
            .partitionBy('Border', 'Measure') \
            .rangeBetween(Window.unboundedPreceding, 0)

    df = df.withColumn('Average', (F.sum('Value').over(window) - df['Value']))

    # Stage 3:
    #   divide by total month from 01/01/1996 -> cast to Long
    df = df.withColumn(
            'Average',
            F.when(df['Average'] > 0, (df['Average'] / ((F.year(df['Date']) - 1996) * 12 + F.month(df['Date']) - 1) * 10 + 5) / 10)
                    .otherwise(0).cast('long')) \
            .orderBy(F.desc('Date'), F.desc('Value'), 'Border', 'Measure') \
            .select('Border', 'Date', 'Measure', 'Value', 'Average')

    # Stage 4:
    #   write to csv file
    df.coalesce(1).write \
            .csv(out_dir,
                 sep=',',
                 mode='overwrite',
                 timestampFormat=dttm_format,
                 header=True)
