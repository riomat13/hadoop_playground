package dataalgorithms.stock.mapreduce;


public class MovingAverage {

    private int count;
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
        if (count < size)
            count++;

        // calculate sum in window by circular array
        sum -= values[index];
        sum += value;
        values[index++] = value;

        // reset index when reach the end of array
        if (index == size)
            index = 0;
    }

    public double getAverage() {
        if (count == 0)
            throw new IllegalArgumentException("No data is given. Can't calculate average");
        return sum / count;
    }
}
