package dataalgorithms.wordcount.mapreduce;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.logging.Logger;


public class WordCounter extends Configured implements Tool {

    private static Logger logger = Logger.getLogger(WordCounter.class.getName());

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setJarByClass(getClass());
        job.setMapperClass(TokenizerMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Configuration reduceConf = new Configuration(false);
        ChainReducer.setReducer(job, TokenizerReducer.class,
                Text.class, IntWritable.class,Text.class, IntWritable.class, reduceConf);

        Configuration topNConf = new Configuration(false);
        ChainReducer.addMapper(job, TopNTokenMapper.class,
                Text.class, IntWritable.class, Text.class, IntWritable.class, topNConf);

        if (args.length > 2) {
            String[] runArgs = new String[2];
            int idx = 0;
            for (int i = 0; i < args.length; i++) {
                if (args[i].equals("-skip")) {
                    job.addCacheFile(new Path(args[++i]).toUri());
                    job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
                }
                else if (args[i].equals("-n")) {
                    int n = Integer.parseInt(args[++i]);
                    job.getConfiguration().setInt("wordcount.N", n);
                } else {
                    try {
                        runArgs[idx++] = args[i];
                    } catch (ArrayIndexOutOfBoundsException err) {
                        // raise if unregistered arguments are given
                        throw new IllegalArgumentException("Usage: wordcount <in> <out> [-skip skipPatternFile] [-n top n tokens (default: 10)]");
                    }
                }
            }
            args = runArgs;
        }

        FileSystem hdfs = FileSystem.get(conf);
        Path out = new Path(args[1]);
        if (hdfs.exists(out))
            hdfs.delete(out, true);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, out);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length % 2 == 1 || args.length > 6) {
            throw new IllegalArgumentException("Usage: wordcount <in> <out> [-skip skipPatternFile] [-n top n tokens (default: 10)]");
        }

        int status = ToolRunner.run(new WordCounter(), args);
        System.exit(status);
    }
}
