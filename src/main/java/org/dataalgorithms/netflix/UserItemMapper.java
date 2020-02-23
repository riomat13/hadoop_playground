package org.dataalgorithms.netflix;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class UserItemMapper extends Mapper<LongWritable, Text, UserMovieCompositeKey, ItemRate> {

    private int currentMovie;
    private final UserMovieCompositeKey userKey = new UserMovieCompositeKey();
    private final ItemRate rate = new ItemRate();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] tokens = value.toString().split(",");

        if (tokens.length == 1) {
            currentMovie = Integer.parseInt(tokens[0].substring(0, tokens[0].length()-1));
        }
        else {
            userKey.setUserId(Integer.parseInt(tokens[0]));
            userKey.setMovieId(currentMovie);
            rate.setId(currentMovie);
            rate.setRate(Integer.parseInt(tokens[1]));
            rate.setRatedAt(tokens[2]);

            context.write(userKey, rate);
        }
    }
}
