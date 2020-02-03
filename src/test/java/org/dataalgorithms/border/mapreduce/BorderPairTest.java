package org.dataalgorithms.border.mapreduce;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BorderPairTest {
    private BorderPair pair = new BorderPair();

    void buildPair() {
        pair.setYearMonth("2015-11");
        pair.setBorder("US-Canada Border");
        pair.setMeasure("Trucks");
        pair.setValue(100);
    }

    @Test
    void testComparePairs() {
        buildPair();

        BorderPair target = new BorderPair();
        // test for equal
        target.setYearMonth(pair.getYearMonth().toString());
        target.setBorder(pair.getBorder().toString());
        target.setMeasure(pair.getMeasure().toString());
        target.setValue(pair.getValue().get());

        assertTrue(pair.equals(target));
        assertTrue(target.equals(pair));

        assertEquals(0, pair.compareTo(target));
        assertEquals(0, target.compareTo(pair));

        // test if target is earlier month with the same year
        target.setYearMonth("2015-10");  // one month earlier
        assertFalse(pair.equals(target));
        assertFalse(target.equals(pair));

        assertEquals(-1, pair.compareTo(target));
        assertEquals(1, target.compareTo(pair));

        // test if target is set as different Border
        target.setYearMonth(pair.getYearMonth().toString());
        target.setBorder("US-Mexico Border");
        assertFalse(pair.equals(target));
        assertFalse(target.equals(pair));

        // Mexico is later than Canada
        assertTrue(pair.compareTo(target) < 0);
        assertTrue(target.compareTo(pair) > 0);
    }
}