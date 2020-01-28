package org.dataalgorithms.border.mapReduce;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;


/** Apply MapReduce to sort data by following:
 *   Key:   Year-Month,Border,Measure
 *   Value: Value
 *
 *   Year-Month is descending order(new->old), otherwise alphabetically ascending order
 */
public class BorderMapReduce extends Configured implements Tool {

    private static Logger logger = LogManager.getLogger(BorderMapReduce.class);

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        // remove output dir already exist
        FileSystem hdfs = FileSystem.get(conf);
        Path out = new Path(args[1]);
        if (hdfs.exists(out))
            hdfs.delete(out, true);

        Job job = Job.getInstance(conf);

        job.setJarByClass(getClass());
        job.setJobName("SortBorderCrossingEntry");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, out);

        job.setMapperClass(BorderMapper.class);
        job.setMapOutputKeyClass(BorderPair.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setCombinerClass(BorderCombiner.class);
        job.setReducerClass(BorderReducer.class);
        job.setPartitionerClass(BorderPartitioner.class);
        job.setGroupingComparatorClass(BorderGroupingComparator.class);

        boolean status = job.waitForCompletion(true);
        logger.info("run(): status=" + status);

        return status ? 0 : 1;
    }

    /** Main MapReduce runner. In order to submit the job, invoke this method.
     *
     * @throws Exception when there are communication problems with the job tracker.
     */
    public static void main(String[] args) throws Exception {
        if (args.length != 2)
            throw new IllegalArgumentException("Usage: BorderMapReduce" +
                                               " <input-path> <output-path>");

        int exitCode = ToolRunner.run(new BorderMapReduce(), args);
        System.exit(exitCode);

    }
}
