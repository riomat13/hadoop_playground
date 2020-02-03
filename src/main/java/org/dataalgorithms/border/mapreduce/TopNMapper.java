package org.dataalgorithms.border.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.time.format.DateTimeFormatter;
import java.io.IOException;

/** Map operation to extract first top N data for all measure in each border.
 *
 *  Input key-value: Text, IntWritable
 *  Output key-value: Text, Text
 *
 */
public class TopNMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss a");
    private final Text keyField = new Text();
    private final IntWritable value = new IntWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        if (tokens[0].equals("Border"))
            return;

        keyField.set(tokens[0] + "," + tokens[2]);
        this.value.set(Integer.parseInt(tokens[3]));

        context.write(keyField, this.value);
    }


}
