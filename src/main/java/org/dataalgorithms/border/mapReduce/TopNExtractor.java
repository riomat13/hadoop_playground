package org.dataalgorithms.border.mapReduce;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;


/** Extracting top N data from processed data */
public class TopNExtractor extends Configured implements Tool {

    private static Logger logger = LogManager.getLogger();

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        FileSystem hdfs = FileSystem.get(conf);
        Path out = new Path(args[1]);
        if (hdfs.exists(out))
            hdfs.delete(out, true);

        Job job = Job.getInstance(conf);

        // if specified number of data to extract, otherwise 10
        if (args.length == 3)
            job.getConfiguration().setInt("N", Integer.parseInt(args[2]));

        job.setJarByClass(getClass());
        job.setJobName("TopNBorderCrossingEntry");

        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, out);

        boolean status = job.waitForCompletion(true);
        logger.info("run(): status=" + status);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();

        Option input = new Option("i", "input", true, "input file path");
        input.setRequired(true);
        options.addOption(input);

        Option output = new Option("o", "output", true, "output file path");
        output.setRequired(true);
        options.addOption(output);

        Option topN = new Option("n", "topn", true, "top N data to extract");
        options.addOption(topN);

        int exitCode = 0;
        CommandLineParser parser = new DefaultParser();

        try {
            CommandLine cmd = parser.parse(options, args);

            String n = cmd.getOptionValue("n");
            String[] args_;

            if (n != null) {
                args_ = new String[] {
                    cmd.getOptionValue("i"),
                    cmd.getOptionValue("o"),
                    n
                };
            }
            else {
                args_ = new String[] {
                    cmd.getOptionValue("i"),
                    cmd.getOptionValue("o")
                };
            }

            exitCode = ToolRunner.run(new TopNExtractor(), args_);
        }
        catch (ParseException exp) {
            System.err.println("Parsing failed: " + exp.getMessage());
            exitCode = 1;
        }
        finally {
            System.exit(exitCode);
        }
    }
}
