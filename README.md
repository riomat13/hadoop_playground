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
 |    |    |     |─org.dataalgorithms.mapReduce  # sinple map-reduce with hadoop
 |    |    |
 |    |    └─ resources  # store
 |    └─ test  # all tests
 |─ pom.xml    # project information
 └─ README.md  # this file

```

## Execute code
Example usage
```bash
export JAVA_HOME=/path/to/jdk

# run with maven (example with border crossing entry data)
mvn exec:java -D exec.mainClass=org.dataalgorithms.border.mapReduce.BorderMapReduce -D exec.args="input output"
```
