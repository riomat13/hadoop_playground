# Data Algorithm Practice

This is for practicing developing hadoop and spark data algorithms project.

## Directory Structure

```$xslt
root
 |─ etc
 |    |─ hadoop     # files used for configurations for hadoop. copy them to $HADOOP_HOME/etc/hadoop/
 |─ input   # store input data
 |─ output  # output data will come here
 |─ src
 |    |─ main
 |    |    |─ java
 |    |    |     |─org.dataalgorithms.border.mapreduce      # Border Crossing Entry
 |    |    |     |─org.dataalgorithms.wordcount.mapreduce   # Tokenize and count words
 |    |    |
 |    |    └─ resources  # store
 |    |─ test  # all tests for java
 |    └─ python
 |         |─ main
 |               |─ settings
 |               |─ border  # border crossing entry with pyspark
 |
 |─ pom.xml    # project build settings
 └─ README.md  # this file

```

## Setting up
Set up with single cluster for standalone and pseudo-distribution:
[link](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html)

### Set up data in hdfs
```bash
## run hadoop cluster
$ hadoop namenode -format
$ hadoop --daemon start namenode
$ hadoop --daemon start datanode
$ yarn --daemon start resourcemanager
$ yarn --daemon start nodemanager

# send data to hdfs
# (change the path in hdfs accordingly)
$ hadoop fs -mkdir -p /user/hdfs/input
$ hadoop fs -put /path/to/dataset input
```

## Border Crossing Entry data processing

### Directory paths
```bash
# Hadoop MapReduce
src/main/java/org/dataalgorithms/border

# Spark (pyspark)
src/python/border
```

Data link: [Border Crossing Entry Data](https://data.transportation.gov/Research-and-Statistics/Border-Crossing-Entry-Data/keg4-3bc2)

Target output is the same as [link](https://github.com/riomat13/border_crossing_analysis)

### Execute code
```bash
# Copy data into hdfs
hdfs dfs -put /path/to/data/* input

# run mapreduce to group by date, border, measure
# the result will be saved as `report.csv`
hadoop jar /path/to/jar org.dataalgorithms.border.mapReduce.Executor input output

# run mapreduce to get top N data from processed data (report.csv)
# (argument -n is optional, default value is 10)
hadoop jar /path/to/jar org.dataalgorithms.border.mapReduce.TopNExtractor -i output/report.csv -o output/topN -n 10

# run with spark on local machine (pyspark)
spark-submit \
    --master local[*] \
    /path/to/spark.py

# if run on cluster
spark-submit \
    --master 'cluster path' \
    /path/to/spark.py \
```

## Word count

Count words based on [tutorial](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
and sorted by the counts, then return top N words with counts..

```bash
# Haoop MapReduce
src/main/java/org/dataalgorithms/wordcount

# execute
hadoop jar /path/to/jar org.dataalgorithms.wordcount.mapreduce.WordCounter <input> <output> [-n 10]
```
