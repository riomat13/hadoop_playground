package org.dataalgorithms.netflix;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

import org.dataalgorithms.util.DateConverter;

/** Reduce by user id and grouping all information into one file */
public class UserGroupingReducer extends Reducer<UserMovieCompositeKey, ItemRate, NullWritable, Text> {

    private final Text content = new Text();
    private final DateConverter converter = new DateConverter("yyyy-mm-dd");

    private MultipleOutputs<NullWritable, Text> mos;

    public void setup(Context context) {
        mos = new MultipleOutputs<>(context);
    }

    public String getFileName(UserMovieCompositeKey key) {
        return String.format("%08d", key.getUserId().get());
    }

    public String convertToCSV(Iterable<ItemRate> items) {
        StringBuffer sb = new StringBuffer();
        for (ItemRate item: items) {
            sb.append(String.format("%d,%d,%s",
                    item.getId().get(),
                    item.getRate().get(),
                    converter.convertToString(item.getRatedAt().get())));
        }
        return sb.toString();
    }

    @Override
    public void reduce(UserMovieCompositeKey key, Iterable<ItemRate> values, Context context)
        throws IOException, InterruptedException {

        content.set(convertToCSV(values));

        mos.write(NullWritable.get(), content, getFileName(key));
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
