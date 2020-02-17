package org.dataalgorithms.stock.mapreduce;

import org.apache.hadoop.mapreduce.Partitioner;

public class StockPartitioner extends Partitioner<CompositeKey, StockData> {

    /** distribute keys with grouping by company codes to make values put into the same reducer */
    @Override
    public int getPartition(CompositeKey key, StockData value, int numberOfPartition) {
       return Math.abs((int) (hash(key.getCode()) % numberOfPartition));
    }

    static long hash(String str) {
        // prime number
        long h = 12582917;
        for (int i = 0; i < str.length(); i++) {
            h = h * 31 + str.charAt(i);
        }
        return h;
    }
}
