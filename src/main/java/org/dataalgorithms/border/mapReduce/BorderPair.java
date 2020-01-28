package org.dataalgorithms.border.mapReduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Objects;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/** Instantiate border data to compare with Hadoop.
 *  The data structure is:
 *      Port Name, State, Port Code, Border, Date, Measure, Value
 *
 *  and target structure is:
 *      Border, Date (Year-Month), Measure, Value, Average Value
 *  which is grouped by Date and Border and Measure.
 *  Average Value will be the average from begging to current Data(Month)
 *
 *  The both input and output data will be CSV format.
 */
public class BorderPair implements Writable, WritableComparable<BorderPair> {

    private Text yearMonth = new Text();
    private Text measure = new Text();
    private Text border = new Text();
    private IntWritable value = new IntWritable();
    private double average;

    public BorderPair() {
    }

    public BorderPair(String yearMonth, String border, String measure, int value) {
        this.yearMonth.set(yearMonth);
        this.border.set(border);
        this.measure.set(measure);
        this.value.set(value);
    }

    public Text getYearMonth() { return yearMonth; }

    public Text getBorder() { return border; }

    public Text getMeasure() { return measure; }

    public IntWritable getValue() { return value; }

    /** Build field name to grouping for reducing
     *
     * @return Text instance with field name
     */
    public Text getKeyField() {
        StringBuilder sb = new StringBuilder();
        sb.append(yearMonth.toString());
        sb.append(",");
        sb.append(border);
        sb.append(",");
        sb.append(measure);

        return new Text(sb.toString());
    }

    public void setYearMonth(String yearMonth) {
        this.yearMonth.set(yearMonth);
    }

    public void setBorder(String border) {
        this.border.set(border);
    }

    public void setMeasure(String measure) {
        this.measure.set(measure);
    }

    public void setValue(int value) {
        this.value.set(value);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        yearMonth.write(out);
        border.write(out);
        measure.write(out);
        value.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        yearMonth.readFields(in);
        border.readFields(in);
        measure.readFields(in);
        value.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        BorderPair that = (BorderPair) o;

        return Objects.equals(yearMonth, that.yearMonth)
                && Objects.equals(border, that.border)
                && Objects.equals(measure, that.measure)
                && Objects.equals(value, that.value);
    }

    /** Sort BorderPair by the order in 'yearMonth', 'border', 'measure', then 'value'
     *  yearMonth and value in descending order, border and measure in ascending order
     *
     * @param pair BorderPair object
     * @return int 1 if this object is smaller, -1 if larger, otherwise 0
     */
    @Override
    public int compareTo(BorderPair pair) {
        int cmp = -1 * this.yearMonth.compareTo(pair.yearMonth);
        if (cmp == 0)
            cmp = border.compareTo(pair.border);
        if (cmp == 0)
            cmp = measure.compareTo(pair.measure);
        if (cmp == 0)
            cmp = -1 * value.compareTo(pair.value);

        return cmp;
    }

    /** Sort by the order in 'yearMonth', 'border', 'measure'.
     *  yearMonth is descending order, otherwise ascending order.
     *
     *  This is used for grouping object for reduce step.
     *
     * @param pair target to compare
     * @return 0 if yearMonth, border, measure are equal, 1 or -1 otherwise accordingly
     */
    protected int compareInGroup(BorderPair pair) {
        int cmp = -1 * this.yearMonth.compareTo(pair.yearMonth);
        if (cmp == 0)
            cmp = border.compareTo(pair.border);
        if (cmp == 0)
            cmp = measure.compareTo(pair.measure);

        return cmp;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BorderPair{yearMonth=");
        sb.append(yearMonth);
        sb.append(", border=");
        sb.append(border);
        sb.append(", measure=");
        sb.append(measure);
        sb.append(", value=");
        sb.append(value);
        sb.append("}");
        return sb.toString();
    }
}
