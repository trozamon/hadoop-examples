package com.alectenharmsel.research;

import com.alectenharmsel.research.MoabLicensesReducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MoabLicensesReducerTest {

    private MoabLicensesReducer reducer;
    private ReduceDriver driver;
    private ArrayList<Text> vals;
    private Text key;
    private List<Pair<Text, Text> > res;

    @Before
    public void setUp() throws IOException {
        reducer = new MoabLicensesReducer();
        driver = new ReduceDriver(reducer);
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
        driver.withInput(key, vals);
        res = driver.run();
    }

    @Test
    public void testResultSize() {
        Assert.assertEquals("Reducer should return a single result",
            1, res.size());
    }

    @Test
    public void testZeroAvailable() {
        String avg = res.get(0).getSecond().toString();
        double tmp = Double.parseDouble(avg.split(",")[0]);
        Assert.assertEquals("The average should be 5/8", 5.0/8.0, tmp, 0.01);
    }

    @Test
    public void testZeroTotal() {
        String total = res.get(0).getSecond().toString();
        double tmp = Double.parseDouble(total.split(",")[1]);
        Assert.assertEquals("The total should be 7.0", 7.0, tmp, 0.01);
    }
}
