package dataalgorithms.netflix;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class UserGroupingDriver extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(getClass());

        job.setMapperClass(UserItemMapper.class);
        job.setOutputKeyClass(UserMovieCompositeKey.class);
        job.setOutputValueClass(ItemRate.class);

        // grouping items with secondary sort by movie ID
        job.setPartitionerClass(UserItemPartitioner.class);
        job.setGroupingComparatorClass(UserItemGroupingComparator.class);

        job.setReducerClass(UserGroupingReducer.class);

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
            throw new IllegalArgumentException("Usage: UserGroupingDriver <input> <output>");

        int status = ToolRunner.run(new UserGroupingDriver(), args);
        System.exit(status);
    }
}
