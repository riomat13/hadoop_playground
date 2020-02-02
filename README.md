# Data Algorithm Practice

This is for practicing developing hadoop and spark data algorithms project.

## Directory Structure

```$xslt
root
 |─ input   # store input data
 |─ output  # output data will come here
 |─ src
 |    |─ main
 |    |    |─ java
 |    |    |     |─org.dataalgorithms.border.mapReduce  # simple map-reduce with hadoop
 |    |    |
 |    |    └─ resources  # store
 |    └─ test  # all tests
 |─ pom.xml    # project information
 └─ README.md  # this file

```

## Setting up
For single cluster: [link](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html)

## Execute code
Example usage:
```bash
# run with maven (local file system)
mvn exec:java -D exec.mainClass="main class" -D exec.args="some args"

# run with hadoop
hadoop jar /path/to/jar "main class" arg1 arg2 ...
```
