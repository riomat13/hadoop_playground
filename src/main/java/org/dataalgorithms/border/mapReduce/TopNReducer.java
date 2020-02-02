package org.dataalgorithms.border.mapReduce;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.util.PriorityQueue;
import java.util.Stack;
import java.io.IOException;


public class TopNReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    private int N = 10;
    private PriorityQueue<Node> queue = new PriorityQueue<>();

    private class Node implements Comparable<Node> {
        String key;
        long value;

        Node(String key, long value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo(Node that) {
            if      (value > that.value) return 1;
            else if (value < that.value) return -1;
            else                         return 0;
        }
    }

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

        // reduce by key ("border,measure")
        long sum = 0;
        for (IntWritable value: values)
            sum += value.get();

        Node node = new Node(key.toString(), sum);
        queue.add(node);
        if (queue.size() > N)
            queue.remove();
    }

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        N = context.getConfiguration().getInt("N", 10);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        // reverse order for large -> small
        Stack<Node> stack = new Stack<>();
        for (Node node: queue)
            stack.push(node);

        while (!stack.isEmpty()) {
            Node node = stack.pop();
            context.write(new Text(node.key), new LongWritable(node.value));
        }
    }
}
