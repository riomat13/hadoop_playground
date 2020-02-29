package dataalgorithms.netflix;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import dataalgorithms.util.DateConverter;

/** Store rate of movies.
 *  This will contain movie ID, responding rate, and rated date as timestamp
 */
public class ItemRate implements WritableComparable<ItemRate> {

    private final DateConverter converter = new DateConverter("yyyy-mm-dd");
    private final IntWritable id = new IntWritable();
    private final IntWritable rate = new IntWritable();
    private final LongWritable ratedAt = new LongWritable();

    @Override
    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        rate.readFields(in);
        ratedAt.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        id.write(out);
        rate.write(out);
        ratedAt.write(out);
    }

    public IntWritable getId() { return id; }
    public IntWritable getRate() { return rate; }
    public LongWritable getRatedAt() { return ratedAt; }

    public void setId(int id) { this.id.set(id); }

    public void setRate(int rate) {
        if (rate < 1 || rate > 5)
            throw new IllegalArgumentException("Rate must be within [1, 5]");

        this.rate.set(rate);
    }

    public void setRatedAt(String date) {
        ratedAt.set(converter.convertToTimestamp(date));
    }

    /** check equality
     *  since the rate should be only one,
     *  return true comparing by only key (item ID) even if rate is different
     *
     * @param o target object to compare
     * @return true if ID is the same, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || this.getClass() != o.getClass()) return false;

        ItemRate that = (ItemRate) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int compareTo(ItemRate that) {
        return id.get() - that.id.get();
    }
}
