package org.dataalgorithms.border.mapReduce;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.mockito.Mock;
import static org.mockito.Mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

@ExtendWith(MockitoExtension.class)
class TopNMapperTest {

    @Mock
    private Mapper.Context mockContext;

    TopNMapper mapper;

    @BeforeEach
    public void setUp() {
        mapper = new TopNMapper();
    }

    @Test
    public void mapperWritingTest() throws IOException, InterruptedException {
        String border = "US-Mexico Border";
        String measure = "trucks";
        int val = 12345;

        Text value = new Text(String.format("%s,01/01/2000 12:00:00 AM,%s,%d,11111", border, measure, val));
        mapper.map(new LongWritable(0), value, mockContext);
        verify(mockContext).write(new Text(border + "," + measure), new IntWritable(val));
    }
}