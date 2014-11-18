package com.alectenharmsel.research;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import com.alectenharmsel.research.SrcTokMapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.io.LongWritable;

public class SrcTokMapperTest {

    private SrcTokMapper mapper;
    private MapDriver driver;

    @Before
    public void setUp() {
        mapper = new SrcTokMapper();
        driver = new MapDriver(mapper);
    }

    @Test
    public void testSpacedIntegerDeclaration() throws IOException {
        driver.withInput(new LongWritable(0), new Text("    int x;"))
            .withOutput(new Text("int"), new LongWritable(1))
            .withOutput(new Text("x"), new LongWritable(1))
            .runTest();
    }

    @Test
    public void testTabbedIntegerDeclaration() throws IOException {
        driver.withInput(new LongWritable(0), new Text("\tint x;"))
            .withOutput(new Text("int"), new LongWritable(1))
            .withOutput(new Text("x"), new LongWritable(1))
            .runTest();
    }
}
