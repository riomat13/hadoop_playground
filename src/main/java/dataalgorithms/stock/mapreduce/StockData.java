package dataalgorithms.stock.mapreduce;

import org.apache.hadoop.io.Writable;

import java.util.Objects;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import dataalgorithms.util.DateConverter;


/** Data structure hold stock data
 *  The data holding are "Company code", "Date", "Stock value at close"
 */
public class StockData implements Writable, Comparable<StockData> {

    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private final DateConverter converter = new DateConverter(dateFormat);
    private long timestamp;
    private double price;

    public static StockData copy(StockData data) {
        return new StockData(data.timestamp, data.price);
    }

    public StockData() {
    }

    public StockData(long timestamp, double price) {
        this.timestamp = timestamp;
        this.price = price;
    }

    /** Build object by date
     *
     * @param date the format is "yyyy-MM-dd"
     * @param price stock price
     */
    public StockData(String date, double price) {
        setTimestampByDate(date);
        this.price = price;
    }

    /** Set timestamp by string
     *
     * @param date format must be "yyyy-MM-dd", otherwise timestamp will be -1
     */
    public void setTimestampByDate(String date) {
        this.timestamp = converter.convertToTimestamp(date);
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public long getTimestamp() { return timestamp; }
    public double getPrice() { return price; }

    public static StockData read(DataInput in) throws IOException {
        StockData data = new StockData();
        data.readFields(in);
        return data;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        timestamp = in.readLong();
        price = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeDouble(price);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;

        StockData that = (StockData) o;

        if (Objects.equals(timestamp, that.timestamp)
                && Objects.equals(price, that.price))
            return true;
        return false;
    }

    @Override
    public int compareTo(StockData that) {
        if      (timestamp > that.timestamp) return 1;
        else if (timestamp < that.timestamp) return -1;
        else                                 return 0;
    }

    @Override
    public String toString() {
        return String.format("%s\t%.3f", converter.convertToString(timestamp), price);
    }
}
