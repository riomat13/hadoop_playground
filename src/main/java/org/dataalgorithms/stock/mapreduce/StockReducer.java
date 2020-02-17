package org.dataalgorithms.stock.mapreduce;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.Text;

import java.util.logging.Logger;
import java.io.IOException;

public class StockReducer extends Reducer<CompositeKey, StockData, Text, StockData> {

    private final Logger log = Logger.getLogger(getClass().getName());

    private final Text outputKey = new Text();
    private int windowSize = 5;

    public void setUp(Context context) {
        windowSize = context.getConfiguration().getInt("windowSize", 5);
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

            // TODO: replace with MultipleOutputs
            context.write(outputKey, outputValue);
        }
    }

}
