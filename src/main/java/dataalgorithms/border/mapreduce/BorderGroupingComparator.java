package dataalgorithms.border.mapreduce;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;

public class BorderGroupingComparator extends WritableComparator {

    public BorderGroupingComparator() {
        super(BorderPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        if (a instanceof BorderPair && b instanceof BorderPair)
            return ((BorderPair) a).compareInGroup((BorderPair) b);

        return super.compare(a, b);
    }
}
