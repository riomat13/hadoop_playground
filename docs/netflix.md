# Netflix Prize Data Processing

Data link: [Netflix Prize Data (kaggle)](https://www.kaggle.com/netflix-inc/netflix-prize-data)

### Purpose
Calculate similarity of movies.

##### Input data structure:

Check the link above

File name: `combined_data_[1-4].txt`

##### Target data structure:

- User data: `{user_id:08d}-r-00000`
- Movie data: `part-r-00000` (original output name from *Hadoop MapReduce*)


```bash
## Group by User
# for each file contains data with following format:
MovieId,Rating,Date(yyyy-mm-dd)

## Group by Movie (1 file)
MovieId userId1,userId2,...
```

### Directory paths
```bash
# Hadoop MapReduce
src/main/java/org/dataalgorithms/netflix

# Spark app for data analysis
src/python/main/netflix
```

### Execute code
```bash
# execute
# hadoop
# for grouping by user
hadoop jar /path/to/jar org.dataalgorithms.netflix.UserGroupingDriver <input> <output>

# for grouping by movie
hadoop jar /path/to/jar org.dataalgorithms.netflix.MovieGroupingDriver <input> <output>

# Spark (example)
# calculate similarities (running on local machine)
spark-submit
    --master 'local[*]' \
    /path/to/dir/main/netflix/app.py \
    --input output/netflix \
    --output output/netflix/similarity \
    [--parquet] \    # save as parquet file (convert to DataFrame)
    [--from-local] \ # read data from local file system
    [--to-hdfs] \    # save to hdfs
    [--gzip]         # compress data
```

### Similarities
#### Cosine Similarity
For now, to make simplify, use following formula to calculate *Cosine Similarity*:

$$(Cosine\ Similarity) =
\dfrac{\sum(mean(ratings_i)\cdot mean(num\_of\_raters_i))}
      {\sqrt{\sum(mean(ratings_i)^2 + mean(num\_of\_raters_i)^2)}}$$

(TODO: change to use rates by each user instead of average of the rates)