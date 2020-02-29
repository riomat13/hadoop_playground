package dataalgorithms.netflix;

import org.apache.hadoop.mapreduce.Partitioner;

public class UserItemPartitioner extends Partitioner<UserMovieCompositeKey, ItemRate> {

    @Override
    public int getPartition(UserMovieCompositeKey key, ItemRate value, int numberOfPartitions) {
        return Math.abs((int) key.getUserId().get() % numberOfPartitions);
    }
}
