package org.dataalgorithms.stock.mapreduce;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class StockDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
       Configuration conf = getConf();
       FileSystem hdfs = FileSystem.get(conf);
       Path out = new Path(args[1]);
       if (hdfs.exists(out))
           hdfs.delete(out, true);

       Job job = Job.getInstance(conf);
       job.setJarByClass(getClass());

        if (args.length == 4) {
            int idx = 0;
            String[] tmpArgs = new String[2];
            for (int i = 0; i < 4; i++) {
                if (args[i].equals("-n")) {
                    job.getConfiguration().setInt("stock.window.size", Integer.parseInt(args[++i]));
                }
                else
                    tmpArgs[idx++] = args[i];
            }
            args = tmpArgs;
        }

       job.setMapperClass(StockMapper.class);
       job.setMapOutputKeyClass(CompositeKey.class);
       job.setMapOutputValueClass(StockData.class);
       job.setReducerClass(StockReducer.class);
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(StockData.class);
       job.setPartitionerClass(StockPartitioner.class);
       job.setGroupingComparatorClass(CodeKeyComparator.class);

       LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, out);

       return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2 && args.length != 4)
            throw new IllegalArgumentException("Usage stock <input> <output> [-n <window size>]");

        int status = ToolRunner.run(new StockDriver(), args);
        System.exit(status);

    }
}
