# Netflix Prize Data Processing

Data link: [Netflix Prize Data (kaggle)](https://www.kaggle.com/netflix-inc/netflix-prize-data)

### Purpose
Calculate similarity of movies.

#### Input data structure:

Check the link above

File name: `combined_data_[1-4].txt`

#### Target data structure:

##### MapReduce
- User data: `{user_id:08d}-r-00000`

```bash
## Group by User
# for each file contains data with following format:
MovieId,Rating,Date(yyyy-mm-dd)
```

- Movie data: `part-r-00000` (original output name from *Hadoop MapReduce*)

```bash
## Group by Movie (1 file)
MovieId userId1,userId2,...
```

##### Similarity Scores
Output format:
```bash
('{movie_id1},{movie_id2}', 'Cosine={cosine_similarity},Jaccard-bin={jaccard_score_bin},Jaccard={jaccard_score}')
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

### Similarity Score
#### Cosine Similarity
Calculate *cosine similarity* based on following formula:

$$CosineSimilarity_{i, j} =
\dfrac{\sum_{n\in{Users}}(rating_{i,n}\cdot rating_{j,n})}
      {\sqrt{\sum_{n\in{Users}}((rating_{i,n})^2 + (rating_{j,n})^2)}},
      \ where\ i, j\in Movies,\ i < j$$

#### Jaccard Index
Calculate *Jaccard index* by binary values (rated or not), and by ratings.

Followings are the formulars for them:

$$JaccardIndex_{bin\ i, j} = 
\dfrac{\sum_{n\in{Users}}(rating_{i,n} > 0)\ \&\ (rating_{j,n} > 0)}
      {\sum_{n\in{Users}}(rating_{i,n} > 0)\ \|\ (rating_{j,n} > 0)}$$

$$JaccardIndex_{i, j} = 
\dfrac{\sum_{n\in{Users}}(rating_{i,n} == rating_{j,n})}
      {\sum_{n\in{Users}}(rating_{i,n} > 0)\ \|\ (rating_{j,n} > 0)}$$

$$(i, j\in Movies,\ i < j)$$