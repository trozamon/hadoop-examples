/* 
 * Copyright 2013 Alec Ten Harmsel
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * MapReduce job to count the lines in a file
 * Used for big jobs
 */

package com.alectenharmsel.research;

import java.util.ArrayList;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.OutputStream;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class MoabLogSearch extends Configured implements Tool
{
    public int run(String[] args) throws Exception
    {
        if(args.length != 2)
        {
            System.err.println("Usage: MoabLogSearch <input> <output>");
            System.exit(-1);
        }

        Job job = new Job(getConf(), "MoabLogSearch");
        job.setJarByClass(MoabLogSearch.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MoabLogSearchMapper.class);
        job.setReducerClass(MoabLogSearchReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        Configuration check = job.getConfiguration();
        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception
    {
        GenericOptionsParser parse = new GenericOptionsParser(new Configuration(), args);
        Configuration conf = parse.getConfiguration();

        int res = ToolRunner.run(conf, new MoabLogSearch(), parse.getRemainingArgs());

        System.exit(res);
    }
}
