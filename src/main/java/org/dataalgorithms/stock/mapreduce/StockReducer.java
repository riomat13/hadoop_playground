package org.dataalgorithms.stock.mapreduce;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class StockReducer extends Reducer<CompositeKey, StockData, Text, StockData> {

    private final Text outputKey = new Text();
    private int windowSize = 5;
    private MultipleOutputs<Text, StockData> mos;

    public void setup(Context context) {
        windowSize = context.getConfiguration().getInt("stock.window.size", 5);
        mos = new MultipleOutputs<>(context);
    }

    @Override
    public void reduce(CompositeKey key, Iterable<StockData> values, Context context)
        throws IOException, InterruptedException {

        outputKey.set(key.getCode());
        StockData outputValue = new StockData();

        MovingAverage movingAverage = new MovingAverage(windowSize);

        for (StockData data : values) {
            movingAverage.addData(data.getPrice());
            outputValue.setPrice(movingAverage.getAverage());
            outputValue.setTimestamp(data.getTimestamp());

            mos.write(outputKey, outputValue, key.getCode());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        mos.close();
    }
}
