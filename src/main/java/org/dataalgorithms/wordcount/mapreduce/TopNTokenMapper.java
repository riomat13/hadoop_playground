package org.dataalgorithms.wordcount.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.Stack;
import java.util.logging.Logger;
import java.io.IOException;

public class TopNTokenMapper extends Mapper<Text, IntWritable, Text, IntWritable> {

    private static Logger logger = Logger.getLogger(TopNTokenMapper.class.getName());
    private int N = 10;
    private SortedMap<Integer, String> topNmap = new TreeMap<>();

    @Override
    public void setup(Context context) {
        N = context.getConfiguration().getInt("wordcount.N", 10);
        logger.info(String.format("Displaying top %d words", N));
    }

    @Override
    public void map(Text key, IntWritable value, Context context) {
        String word = key.toString();
        int count = value.get();

        topNmap.put(count, word);
        if (topNmap.size() > N)
            topNmap.remove(topNmap.firstKey());
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        Stack<String> words = new Stack<>();
        Stack<Integer> counts = new Stack<>();

        for (Map.Entry<Integer, String> items: topNmap.entrySet()) {
            words.push(items.getValue());
            counts.push(items.getKey());
        }
        while (!words.isEmpty()) {
            context.write(new Text(words.pop()), new IntWritable(counts.pop()));
        }
    }
}
