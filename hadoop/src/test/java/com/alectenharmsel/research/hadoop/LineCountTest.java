package com.alectenharmsel.research.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LineCountTest {

    private MapDriver mapDriver;
    private ReduceDriver reduceDriver;
    private ArrayList<LongWritable> vals;
    private Text key;
    private List<Pair<Text, LongWritable> > res;

    @Before
    public void setUp() {
        mapDriver = new MapDriver(new LineCount.Map());
        reduceDriver = new ReduceDriver(new LineCount.Reduce());

        mapDriver
            .withInput(new Text("heyo"),
                    new Text("lol more lines\n"))
            .withInput(new Text("heyo2"), new Text("boring line contents"));

        key = new Text("heyo");
        vals = new ArrayList<LongWritable>();
        vals.add(new LongWritable(10));
        vals.add(new LongWritable(100));
        reduceDriver.withInput(key, vals);
    }

    @Test
    public void testTwoMapOutputs() throws IOException {
        res = mapDriver.run();
        Assert.assertEquals("Map should have 2 outputs", 2, res.size());
    }

    @Test
    public void testFirstMapOutput() throws IOException {
        res = mapDriver.run();
        Assert.assertEquals("First map output should be 1", (long) 1,
                res.get(0).getSecond().get());
    }

    @Test
    public void testSecondMapOutput() throws IOException {
        res = mapDriver.run();
        Assert.assertEquals("Second map output should be 1", (long) 1,
                res.get(1).getSecond().get());
    }

    @Test
    public void testReduceSize() throws IOException {
        res = reduceDriver.run();
        Assert.assertEquals("Single reduce output", 1, res.size());
    }

    @Test
    public void testReduceOutput() throws IOException {
        res = reduceDriver.run();
        Assert.assertEquals("Reducers simply sums", (long) 110,
                res.get(0).getSecond().get());
    }
}
