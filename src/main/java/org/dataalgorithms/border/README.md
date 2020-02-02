# Border Crossing Entry data processing

This is data processing project by aggregating data with Hadoop or Spark,
then process smaller data by simple Java scripts.

Data link: [Border Crossing Entry Data](https://data.transportation.gov/Research-and-Statistics/Border-Crossing-Entry-Data/keg4-3bc2)

Target output is the same as [link](https://github.com/riomat13/border_crossing_analysis)


## Strucure
```$xslt
- mapReduce: process by Hadoop mapreduce on hdfs
```

## execute code
```bash
# Copy data into hdfs
hdfs dfs -put /path/to/data/* input

# run mapreduce to group by date, border, measure
# the result will be saved as `report.csv`
hadoop jar /path/to/jar org.dataalgorithms.border.mapReduce.Executor input output

# run mapreduce to get top N data from processed data (report.csv)
# (argument -n is optional, default value is 10)
hadoop jar /path/to/jar org.dataalgorithms.border.mapReduce.TopNExtractor -i output/report.csv -o output/topN -n 10
```
