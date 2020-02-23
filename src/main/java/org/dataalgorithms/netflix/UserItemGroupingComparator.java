package org.dataalgorithms.netflix;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class UserItemGroupingComparator extends WritableComparator {

    public UserItemGroupingComparator() {
        super(UserMovieCompositeKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        UserMovieCompositeKey item1 = (UserMovieCompositeKey) w1;
        UserMovieCompositeKey item2 = (UserMovieCompositeKey) w2;
        return item1.compareTo(item2);
    }
}
