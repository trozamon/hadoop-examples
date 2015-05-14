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
 * FileCombine
 *
 * Used to combine large files
 */

package com.alectenharmsel.examples.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FileCombine extends Configured implements Tool
{
    public class Map extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        public void map(LongWritable key, Text contents, Context context) throws IOException, InterruptedException
        {
            context.write(contents, key);
        }
    }

    public class Reduce extends Reducer<Text, LongWritable, Text, LongWritable>
    {
        public void reduce(Text key, Iterable<LongWritable> vals, Context context) throws IOException, InterruptedException
        {
            for(LongWritable tmp:vals)
            {
                context.write(key, tmp);
            }
        }
    }

    public int run(String[] args) throws Exception
    {
        if(args.length != 2)
        {
            System.err.println("Usage: FileCombine <input> <output>");
            System.exit(-1);
        }

        Job job = new Job(getConf(), "FileCombine");
        job.setJarByClass(FileCombine.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception
    {
        GenericOptionsParser parse = new GenericOptionsParser(new Configuration(), args);
        Configuration conf = parse.getConfiguration();

        int res = ToolRunner.run(conf, new FileCombine(), parse.getRemainingArgs());

        System.exit(res);
    }
}
