package dataalgorithms.border.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.util.HashMap;
import java.util.PriorityQueue;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;


public class BorderDataProcessor {

    // data is collected from Jan. 1996
    private static final int STARTYEAR = 1996;

    private static double calculateAverage(String yearMonth, int sum) {
        String[] items = yearMonth.split("-");

        double totalMonth = (Integer.parseInt(items[0]) - STARTYEAR) * 12 + Integer.parseInt(items[1]);
        return sum / totalMonth;
    }

    /** process data by adding average of cumulative sum until current month.
     *
     * @param in Input directory
     * @param out Output directory
     * @throws IOException
     */
    public static void run(String in, String out) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(in);
        FileSystem fs = path.getFileSystem(conf);
        PathFilter filter = new PathFilter() {
            public boolean accept(Path file) {
                return file.getName().startsWith("part");
            }
        };
        FileStatus[] status = fs.listStatus(path, filter);
        Path[] listedPaths = FileUtil.stat2Paths(status);


        HashMap<String, Long> hmap = new HashMap<>();
        PriorityQueue<BorderData> queue = new PriorityQueue<>();
        String header = "Border,Date,Measure,Value,Average\n";
        Path outPath = new Path(out + "/report.csv");
        FSDataOutputStream outputStream = fs.create(outPath);
        outputStream.write(header.getBytes());

        try {
            for (Path p : listedPaths) {

                BufferedReader bf = new BufferedReader(new InputStreamReader(fs.open(p)));
                String line;
                line = bf.readLine();
                while (line != null) {
                    String[] tokens = line.split("\t");
                    String[] keyTokens = tokens[0].split(",");

                    String yearMonth = keyTokens[0];
                    String keyField = String.format("%s,%s", keyTokens[1], keyTokens[2]);

                    int value = Integer.parseInt(tokens[1]);
                    long sum = hmap.getOrDefault(keyField, new Long(0));
                    BorderData data = new BorderData(yearMonth, keyField, value, sum);
                    hmap.put(keyField, sum + value);
                    queue.add(data);

                    line = bf.readLine();
                }

                while (!queue.isEmpty()) {
                    line = queue.remove().form();
                    outputStream.write(line.getBytes());
                }
            }
        }
        finally {
            outputStream.close();
            fs.close();
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2)
            throw new IllegalArgumentException("Usage: BorderDataProcessor <input path> <output path>");

        run(args[0], args[1]);
    }
}
