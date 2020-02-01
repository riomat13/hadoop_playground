# Border Crossing Entry data processing

This is data processing project by aggregating data with Hadoop or Spark,
then process smaller data by simple Java scripts.

Data link: [Border Crossing Entry Data](https://data.transportation.gov/Research-and-Statistics/Border-Crossing-Entry-Data/keg4-3bc2)


## Strucure
```$xslt
- mapReduce: Hadoop mapreduce
```

## execute code
```bash
export JAVA_HOME=/path/to/jdk

# Copy data into hdfs
hdfs dfs -put /path/to/data/* input
hadoop jar /path/to/jar org.dataalgorithms.border.mapReduce.BorderMapReduce input output

# run locally with maven for processing larget data
mvn exec:java -D exec.mainClass=org.dataalgorithms.border.mapReduce.BorderMapReduce -D exec.args="input processed"

```
