# Border Crossing Entry data processing

Data link: [Border Crossing Entry Data](https://data.transportation.gov/Research-and-Statistics/Border-Crossing-Entry-Data/keg4-3bc2)


## Strucure
```$xslt
- mapReduce: Hadoop mapreduce
```

## execute code
```bash
export JAVA_HOME=/path/to/jdk

# run with maven 
mvn exec:java -D exec.mainClass=org.dataalgorithms.border.mapReduce.BorderMapReduce -D exec.args="input output"
```
