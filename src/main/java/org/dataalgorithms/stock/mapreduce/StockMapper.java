package org.dataalgorithms.stock.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;


/** Mapper for stock data
 *
 *  Input format:
 *      Date,Open,High,Low,Close,Volume,OpenInt
 *  Output format:
 *      Text(date,company-name), DoubleWritable(price)
 */
public class StockMapper extends Mapper<LongWritable, Text, CompositeKey, StockData> {

    private final CompositeKey compositeKey = new CompositeKey();
    private final StockData stock = new StockData();

    /** extract company codes from file names
     *  the file name format is assumed as:
     *      `code.us.txt`
     *
     * @param context Mapper context used for map()
     * @return company code
     */
    private String getCompanyCode(Context context) {
        String code = ((FileSplit) context.getInputSplit()).getPath().getName();
        return code.split("\\.")[0];
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] tokens = value.toString().split(",");
        // skip header
        if (tokens[0].equals("Date")) return;

        String code = getCompanyCode(context);

        stock.setTimestampByDate(tokens[0]);
        stock.setPrice(Double.parseDouble(tokens[4]));

        compositeKey.set(code, stock.getTimestamp());

        context.write(compositeKey, stock);
    }
}
