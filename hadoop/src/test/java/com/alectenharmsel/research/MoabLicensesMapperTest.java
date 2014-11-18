package com.alectenharmsel.research;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import java.util.List;
import org.apache.hadoop.mrunit.types.Pair;
import com.alectenharmsel.research.MoabLicensesMapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import org.apache.hadoop.io.LongWritable;

public class MoabLicensesMapperTest {

    private MoabLicensesMapper mapper;
    private MapDriver driver;

    @Before
    public void setUp() {
        mapper = new MoabLicensesMapper();
        driver = new MapDriver(mapper);
    }

    @Test
    public void testNoOutputNeeded() throws IOException {
        List<Pair<Text, Text> > res = driver.withInput(new LongWritable(0),
            new Text(
                "05/11 22:58:25  MNodeUpdateResExpression(nyx5624,FALSE,TRUE)"
                )
            )
            .run();
        Assert.assertTrue(res.isEmpty());
    }

    @Test
    public void testLicenseLine() throws IOException {
        driver.withInput(new LongWritable(0),
            new Text(
                "05/11 22:58:25  INFO:     License cfd_solv_ser        0 of   6 available  (Idle: 33.3%  Active: 66.67%)"
                )
            )
            .withOutput(new Text("cfd_solv_ser-05-11"), new Text("0,6"))
            .runTest();
    }
}
