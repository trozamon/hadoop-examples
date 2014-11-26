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
        mapDriver = new MapDriver(new MoabLicenseInfo().new Map());
        reduceDriver = new ReduceDriver(new MoabLicenseInfo().new Reduce());

        mapDriver
            .withInput(new Text("heyo"), new Text("boring line contents"))
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
        Assert.assertEquals("Map should have 2 outputs", res.size(), 2);
    }
}
