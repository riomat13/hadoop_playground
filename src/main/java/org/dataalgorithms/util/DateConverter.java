package org.dataalgorithms.util;

import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

/** Convert Date or string to timestamp and vice versa.
 *  This can be used both in static method or object method.
 */
public class DateConverter {

    private DateFormat formatter;

    /** Convert Date object to String by given format.
     *
     * @param date Date object
     * @param pattern date format, i.e. 'yyyy-MM-dd'
     * @return formatted string by the pattern above
     */
    public static String convertToString(Date date, String pattern) {
        DateFormat formatter = new SimpleDateFormat(pattern);
        return formatter.format(date);
    }

    /** Convert timestamp value to String by given format.
     *
     * @param timestamp long value representing time
     * @param pattern date format, i.e. 'yyyy-MM-dd'
     * @return formatted string by the pattern above
     */
    public static String convertToString(long timestamp, String pattern) {
        Date date = new Date(timestamp);
        DateFormat formatter = new SimpleDateFormat(pattern);
        return formatter.format(date);
    }

    public static long convertToTimestamp(String date, String pattern) {
        DateFormat formatter = new SimpleDateFormat(pattern);
        try {
            return formatter.parse(date).getTime();
        } catch (Exception e) {
            return -1;
        }
    }

    public DateConverter(String pattern) {
        formatter = new SimpleDateFormat(pattern);
    }

    public DateConverter(DateFormat formatter) {
        this.formatter = formatter;
    }

    public void setFormatter(DateFormat formatter) {
        this.formatter = formatter;
    }

    public String convertToString(long timestamp) {
        Date date = new Date(timestamp);
        return formatter.format(date);
    }

    public String convertToString(Date date) {
        return formatter.format(date);
    }

    public long convertToTimestamp(String date) {
        try {
            long timestamp = formatter.parse(date).getTime();
            return timestamp;
        } catch (Exception e) {
            // handle error
        }
        return -1;
    }
}
