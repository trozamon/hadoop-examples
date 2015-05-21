package com.alectenharmsel.examples.hadoop;

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

public class BAMSummaryTest
{
    private MapDriver mapDriver;
    private ReduceDriver reduceDriver;
    private ArrayList<Text> vals;
    private Text key;
    private List<Pair<Text, Text> > res;

    @Test
    public void testNoOutputNeeded() throws IOException {
        Assert.assertEquals("SRR062641", BAMSummary.getRecordName("SRR062641"));
    }
}
