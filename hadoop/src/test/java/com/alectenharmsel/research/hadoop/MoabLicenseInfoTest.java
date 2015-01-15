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

public class MoabLicenseInfoTest {

    private MapDriver mapDriver;
    private ReduceDriver reduceDriver;
    private ArrayList<Text> vals;
    private Text key;
    private List<Pair<Text, Text> > res;

    @Before
    public void setUp() {
        mapDriver = new MapDriver(new MoabLicenseInfo.Map());
        reduceDriver = new ReduceDriver(new MoabLicenseInfo.Reduce());

        key = new Text("cfd_solv_ser-05-11");
        vals = new ArrayList<Text>();
        vals.add(new Text("0,6"));
        vals.add(new Text("0,6"));
        vals.add(new Text("0,6"));
        vals.add(new Text("0,6"));
        vals.add(new Text("2,8"));
        vals.add(new Text("2,8"));
        vals.add(new Text("1,8"));
        vals.add(new Text("0,8"));
        reduceDriver.withInput(key, vals);
    }

    @Test
    public void testNoOutputNeeded() throws IOException {
        List<Pair<Text, Text> > res = mapDriver.withInput(new LongWritable(0),
            new Text(
                "05/11 22:58:25  MNodeUpdateResExpression(nyx5624,FALSE,TRUE)"
                )
            )
            .run();
        Assert.assertTrue(res.isEmpty());
    }

    @Test
    public void testLicenseLine() throws IOException {
        mapDriver.withInput(new LongWritable(0),
            new Text(
                "05/11 22:58:25  INFO:     License cfd_solv_ser        0 of   6 available  (Idle: 33.3%  Active: 66.67%)"
                )
            )
            .withOutput(new Text("cfd_solv_ser-05-11"), new Text("0,6"))
            .runTest();
    }

    @Test
    public void testResultSize() throws IOException {
        res = reduceDriver.run();
        Assert.assertEquals("Reducer should return a single result",
            1, res.size());
    }

    @Test
    public void testZeroAvailable() throws IOException {
        res = reduceDriver.run();
        String avg = res.get(0).getSecond().toString();
        double tmp = Double.parseDouble(avg.split(",")[0]);
        Assert.assertEquals("The average should be 5/8", 5.0/8.0, tmp, 0.01);
    }

    @Test
    public void testZeroTotal() throws IOException {
        res = reduceDriver.run();
        String total = res.get(0).getSecond().toString();
        double tmp = Double.parseDouble(total.split(",")[1]);
        Assert.assertEquals("The total should be 7.0", 7.0, tmp, 0.01);
    }

    @Test
    public void testZeroKey() throws IOException {
        res = reduceDriver.run();
        String key = res.get(0).getFirst().toString();
        Assert.assertEquals("The key should be properly formatted",
            "05-11,cfd_solv_ser", key);
    }
}
