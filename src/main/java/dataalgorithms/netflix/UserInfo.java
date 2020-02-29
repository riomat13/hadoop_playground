package dataalgorithms.netflix;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SortedMapWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/** Used for store Movie ID and rate combinations for each user */
public class UserInfo implements Writable {

    private final IntWritable userId = new IntWritable();
    private final IntWritable rate = new IntWritable();
    private final SortedMapWritable itemMap = new SortedMapWritable();
    private int count = 0;

    public UserInfo() {
    }

    public UserInfo(int userId) {
        this.userId.set(userId);
    }

    public int getUserId() { return this.userId.get(); }

    /** Get item rated by the user
     *
     * @param movieId target movie
     * @return rate if rated by user, otherwise return 0
     */
    public int getItemRate(int movieId) {
        int rate;
        try {
            IntWritable writableRate = (IntWritable) this.itemMap.get(movieId);
            rate = writableRate.get();
        }
        catch (ClassCastException | NullPointerException e) {
            rate = 0;
        }
        return rate;
    }

    public void putAll(SortedMapWritable that) {
        this.itemMap.putAll(that);
    }

    public void setUserId(int id) { userId.set(id); }

    /** Put item to map
     *
     * @param key   key id representing movie id
     * @param value value representing rating
     */
    public void putItem(int key, int value) {
        IntWritable writableKey = new IntWritable(key);
        IntWritable writableValue = new IntWritable(value);
        itemMap.put(writableKey, writableValue);
        count++;
    }

    public void putItem(IntWritable key, IntWritable value) {
        itemMap.put(key, value);
        count++;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        rate.readFields(in);
        itemMap.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // set current total movie items rated by the user
        rate.set(count);
        rate.write(out);
        itemMap.write(out);
    }
}
