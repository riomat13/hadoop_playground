package org.dataalgorithms.netflix;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.LongWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserMovieCompositeKey implements WritableComparable<UserMovieCompositeKey> {

    private final LongWritable userId = new LongWritable();
    private final LongWritable movieId = new LongWritable();

    public LongWritable getUserId() { return userId; }
    public LongWritable getMovieId() { return movieId; }

    public void setUserId(int userId) {
        this.userId.set(userId);
    }

    public void setMovieId(int movieId) {
        this.movieId.set(movieId);
    }

    @Override
    public void readFields(DataInput in) throws IOException  {
        userId.readFields(in);
        movieId.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        userId.write(out);
        movieId.write(out);
    }

    @Override
    public int compareTo(UserMovieCompositeKey that) {
        int cmp = userId.compareTo(that.userId);
        if (cmp == 0)
            cmp = movieId.compareTo(that.movieId);

        return cmp;
    }
}
