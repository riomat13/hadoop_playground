package dataalgorithms.stock.mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.util.Objects;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class CompositeKey implements WritableComparable<CompositeKey> {

    private String code;
    private long timestamp;

    public static CompositeKey copy(CompositeKey obj) {
        return new CompositeKey(obj.code, obj.timestamp);
    }

    public CompositeKey() {
    }

    public CompositeKey(String code, long timestamp ) {
        this.code = code;
        this.timestamp = timestamp;
    }

    public String getCode() { return code; }
    public long getTimestamp() { return timestamp; }

    public void set(String code, long timestamp) {
        this.code = code;
        this.timestamp = timestamp;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        code = in.readUTF();
        timestamp = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(code);
        out.writeLong(timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        CompositeKey that = (CompositeKey) o;

        return Objects.equals(code, that.code)
                && timestamp == that.timestamp;
    }

    @Override
    public int compareTo(CompositeKey that) {
        int cmp = code.compareTo(that.code);
        if (cmp == 0)
            if (timestamp > that.timestamp)      return 1;
            else if (timestamp < that.timestamp) return -1;
            else                                 return 0;
        return cmp;
    }

    @Override
    public String toString() {
        return String.format("CompositeKey(code=%s,timestamp=%d)", code, timestamp);
    }
}
