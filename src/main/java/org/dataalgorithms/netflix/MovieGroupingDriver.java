package org.dataalgorithms.netflix;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.util.logging.Logger;

public class MovieGroupingDriver extends Configured implements Tool {

    private static final Logger logger = Logger.getLogger(MovieGroupingDriver.class.getName());

    private static class MovieGroupingMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

        private final LongWritable movieId = new LongWritable();
        private final LongWritable userId = new LongWritable();
        private long current_id = 0;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] tokens = value.toString().split(",");
            if (tokens.length == 1)
                current_id = Long.parseLong(tokens[0].substring(0, tokens[0].length() - 1));
            else {
                movieId.set(current_id);
                userId.set(Long.parseLong(tokens[0]));
                context.write(movieId, userId);
            }
        }
    }

    private static class MovieGroupingReducer extends Reducer<LongWritable, LongWritable, LongWritable, Text> {

        private final Text users = new Text();

        /** write user Ids for output */
        private void setItems(Iterable<LongWritable> userIds) {
            StringBuilder sb = new StringBuilder();

            for (LongWritable id: userIds) {
                sb.append(id.toString());
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);

            users.set(sb.toString());
        }

        @Override
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            setItems(values);
            context.write(key, users);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(getClass());

        job.setMapperClass(MovieGroupingMapper.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setReducerClass(MovieGroupingReducer.class);

        FileSystem hdfs = FileSystem.get(conf);
        Path out = new Path(args[1]);
        if (hdfs.exists(out))
            hdfs.delete(out, true);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2)
            throw new IllegalArgumentException("Usage: MovieGroupingDriver <input> <output>");

        int status = ToolRunner.run(new MovieGroupingDriver(), args);
        System.exit(status);
    }
}
