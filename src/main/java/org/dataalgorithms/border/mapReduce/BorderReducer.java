package org.dataalgorithms.border.mapReduce;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;

public class BorderReducer extends Reducer<BorderPair, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(BorderPair key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value: values)
            sum += value.get();

        context.write(key.getKeyField(), new IntWritable(sum));
    }
}
