package dataalgorithms.mag;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.json.JSONObject;

import java.util.Iterator;
import java.io.IOException;

public class YearCounter extends Configured implements Tool {

    private static class CountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private final Text year = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject obj = new JSONObject(value.toString());
            year.set(obj.get("year").toString());
            context.write(year, NullWritable.get());
        }
    }

    /* Count by published year of papers */
    private static class CountReducer extends Reducer<Text, NullWritable, Text, LongWritable> {

        private final LongWritable count = new LongWritable();

        @Override
        public void reduce(Text key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {

            int cnt = 0;
            for (NullWritable n: values) cnt++;

            count.set(cnt);
            context.write(key, count);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        FileSystem hdfs = FileSystem.get(conf);
        Path out = new Path(args[1]);
        // delete if already exists
        if (hdfs.exists(out))
            hdfs.delete(out, true);

        Job job = Job.getInstance(conf);
        job.setJarByClass(getClass());
        job.setJobName("PaperPublishedYearCount");

        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, out);

        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // args => (input path, output path)
        int exitCode = ToolRunner.run(new YearCounter(), args);
        System.exit(exitCode);
    }
}
