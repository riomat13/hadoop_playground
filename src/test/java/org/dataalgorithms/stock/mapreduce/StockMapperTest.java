package org.dataalgorithms.stock.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.dataalgorithms.util.DateConverter.*;


@ExtendWith(MockitoExtension.class)
class StockMapperTest {

    @Mock
    private Mapper.Context mockContext;

    StockMapper mapper;

    /** used for return object of context.getInputSplit() */
    @Mock
    private FileSplit mockSplit;

    /** used for FileSplit().getPath() */
    @Mock
    private Path mockPath;

    @BeforeEach
    public void setUp() { mapper = new StockMapper(); }

    @Test
    public void stockMapperWritingTest() throws IOException, InterruptedException  {
        String date = "2010-07-21";
        String price = "23.946";
        long timestamp = convertToTimestamp(date, "yyyy-MM-dd");
        Text input = new Text(String.format("%s,24.333,24.333,23.946,%s,43321,0", date, price));

        StockData target = new StockData();
        target.setTimestampByDate(date);
        target.setPrice(Double.parseDouble(price));

        String code = "test";
        when(mockContext.getInputSplit()).thenReturn(mockSplit);
        when(mockSplit.getPath()).thenReturn(mockPath);
        when(mockPath.getName()).thenReturn(code);

        mapper.map(new LongWritable(0), input, mockContext);
        verify(mockContext).write(new CompositeKey(code, timestamp), target);
    }

}