package dataalgorithms.border.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class BorderPartitioner extends Partitioner<BorderPair, Text> {

    /** Get partition index by yearMonth in BorderPair object
     *
     * @param pair BorderPair object
     * @param text
     * @param numberOfPartitions number of partitions to divide
     * @return partition number
     */
    @Override
    public int getPartition(BorderPair pair, Text text, int numberOfPartitions) {
        return Math.abs(pair.getYearMonth().hashCode() % numberOfPartitions);
    }
}
