package org.dataalgorithms.wordcount.mapreduce;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;


public class TokenizerReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable count = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable val: values)
            sum += val.get();

        count.set(sum);
        context.write(key, count);
    }
}
