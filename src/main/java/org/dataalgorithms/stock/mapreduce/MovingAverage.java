package org.dataalgorithms.stock.mapreduce;

import java.util.logging.Logger;

public class MovingAverage {

    private final Logger logger = Logger.getLogger(getClass().getName());
    private final int size;
    private int index = 0;
    private double sum = 0.0;
    private double[] values;

    public MovingAverage(int size) {
        if (size < 1)
            throw new IllegalArgumentException("Size must be positive");
        this.size = size;
        values = new double[size];
    }

    public void addData(double value) {
        sum -= values[index];
        sum += value;
        values[index++] = value;

        // reset index when reach the end of array
        if (index == size)
            index = 0;
    }

    public double getAverage() {
        return sum / size;
    }
}
