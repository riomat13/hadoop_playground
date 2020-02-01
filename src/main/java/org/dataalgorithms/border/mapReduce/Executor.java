package org.dataalgorithms.border.mapReduce;

import org.apache.hadoop.util.ToolRunner;

public class Executor {

    public static void main(String[] args) throws Exception {
        if (args.length != 2)
            throw new IllegalArgumentException("Usage: Executor <input dir> <output dir>");

        String[] mrArgs = new String[] {args[0], "processed"};
        ToolRunner.run(new BorderMapReduce(), mrArgs);
        BorderDataProcessor.run("processed", args[1]);
    }
}
