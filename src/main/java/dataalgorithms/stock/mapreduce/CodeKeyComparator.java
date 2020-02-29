package dataalgorithms.stock.mapreduce;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CodeKeyComparator extends WritableComparator {

    protected CodeKeyComparator() {
        super(CompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        CompositeKey c1 = (CompositeKey) w1;
        CompositeKey c2 = (CompositeKey) w2;

        // compare by company code for grouping
        return c1.getCode().compareTo(c2.getCode());
    }
}
