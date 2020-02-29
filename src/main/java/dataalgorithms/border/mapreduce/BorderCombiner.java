package dataalgorithms.border.mapreduce;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;


public class BorderCombiner extends Reducer<BorderPair, IntWritable, BorderPair, IntWritable> {

    @Override
    public void reduce(BorderPair key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value: values)
            sum += value.get();

        context.write(key, new IntWritable(sum));
    }
}
