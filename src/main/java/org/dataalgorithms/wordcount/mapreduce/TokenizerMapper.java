package org.dataalgorithms.wordcount.mapreduce;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.conf.Configuration;


import java.util.Set;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.net.URI;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


/** Tokenize words with configuration.
 *
 *  Settings:
 *      wordcount.case.sensitive: boolean, true and distinguish upper case and lower case, false otherwise
 *      wordcount.punct.remove: boolean, true and remove all punctuation, false otherwise
 *      wordcount.skip.patterns: boolean, true and load file to save patterns to skip, false otherwise
 *
 *  Structure is based on
 *      https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
 */
public class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Logger logger = LogManager.getLogger();

    // used for counter name to extract Counter object by context
    static enum CountersEnum { INPUT_WORDS }

    private final Text word = new Text();
    // all initial counts are set to 1
    private final IntWritable one = new IntWritable(1);

    private boolean caseSensitive;
    private boolean removePunct;
    private Set<String> skipPatterns = new HashSet<>();

    private Configuration conf;
    private BufferedReader buff;

    /** parse file containing patterns to skip while parsing target files in mapper */
    private void parseSkipPatternFile(String fileName) {
        try {
            buff = new BufferedReader(new FileReader(fileName));
            String pattern = null;
            while ((pattern = buff.readLine()) != null) {
                skipPatterns.add(pattern);
            }
        } catch (IOException err) {
            logger.error("Caught exception while parsing the cached file"
                    + StringUtils.stringifyException(err));
        }
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        // default is case sensitive
        caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
        // to simplify the results removing punctuation is initial setting
        removePunct = conf.getBoolean("wordcount.punct.remove", true);

        // if set skip words, parse the files to store all skip words
        if (conf.getBoolean("wordcount.skip.patterns", false)) {
            URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
            for (URI patternsURI : patternsURIs) {
                Path filePath = new Path(patternsURI.getPath());
                parseSkipPatternFile(filePath.getName().toString());
            }
        }
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");

        String line = value.toString();
        if (!caseSensitive)
            line = line.toLowerCase();

        if (removePunct)
            line = line.replaceAll("[\\W&&\\S]", "");

        for (String pattern: skipPatterns)
            line = line.replace(pattern, "");
        StringTokenizer itr = new StringTokenizer(line);

        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
            Counter counter = context.getCounter(
                    CountersEnum.class.getName(),
                    CountersEnum.INPUT_WORDS.toString());
            counter.increment(1);
        }
    }
}
