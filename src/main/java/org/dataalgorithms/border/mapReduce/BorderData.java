package org.dataalgorithms.border.mapReduce;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class BorderData implements Comparable<BorderData> {

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy hh:mm:ss a");
    private final int year;
    private final int month;
    private final String keyField;
    private final int value;
    private final long average;

    public BorderData(String yearMonth, String keyField, int value, long cumulativeSum) throws IOException {
        this.keyField = keyField;
        this.value = value;

        String[] items = yearMonth.split("-");
        year = Integer.parseInt(items[0]);
        month = Integer.parseInt(items[1]);

        int totalMonths = (year - 1996) * 12 + month - 1;
        if (totalMonths == 0)
            average = 0;
        else {
            long average = cumulativeSum * 10 / totalMonths;
            this.average = (average + 5) / 10;
        }

    }

    /** Output form for writing to file.
     *  Format is: border, datetime, measure, value, average
     *  All values are comma separated.
     *
     * @return formatted string
     */
    protected String form() {
        LocalDateTime datetime = LocalDateTime.of(year, month, 1, 0, 0, 0);
        String[] keys = keyField.split(",");

        return String.format("%s,%s,%s,%d,%d\n", keys[0], formatter.format(datetime), keys[1], value, average);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BorderData that = (BorderData) o;

        if (value == that.value
                && average == that.average
                && keyField.equals(that.keyField))
            return true;
        return false;
    }

    /** order by date, value, border, measure
     *  date and value are sorted by descending order
     *
     * @param that target BorderData object to compare
     * @return int value, positive if larger than target, negative if smaller, otherwise 0
     */
    @Override
    public int compareTo(BorderData that) {
        int cmp = that.year - year;
        if (cmp == 0)
            cmp = that.month - month;
        if (cmp == 0) {
            cmp = that.value - value;
        }
        if (cmp == 0) {
            cmp = keyField.compareTo(that.keyField);
        }
        return cmp;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BorderData{yearMonth=");
        sb.append(year + "-" + month);
        sb.append(", keyField=");
        sb.append(keyField);
        sb.append(", value=");
        sb.append(value);
        sb.append(", average=");
        sb.append(average);
        sb.append("}");
        return sb.toString();
    }

}
