package dataalgorithms.border.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.time.LocalDate;

public class BorderMapper extends Mapper<LongWritable, Text, BorderPair, IntWritable>{

    private final BorderPair pair = new BorderPair();
    private final IntWritable borderValue = new IntWritable();
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss a");

    /** Parse input line and mapping into BorderPair
     *  Input line form is:
     *      Port name, State, Port Code, Border, Data, Measure, Value
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        String[] tokens = line.split(",");
        String datetime = tokens[4];
        String border = tokens[3];
        String measure = tokens[5];
        int val = Integer.parseInt(tokens[6]);

        LocalDate date = LocalDate.parse(datetime, formatter);

        String yearMonth = String.format("%d-%02d", date.getYear(), date.getMonthValue());

        pair.setYearMonth(yearMonth);
        pair.setBorder(border);
        pair.setMeasure(measure);
        pair.setValue(val);

        borderValue.set(val);

        context.write(pair, borderValue);
    }
}
