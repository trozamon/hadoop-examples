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

public class CodeTokenizer extends Configured implements Tool
{

    public class Map extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        private final LongWritable one = new LongWritable(1);

        public void map(LongWritable key, Text contents, Context context) throws IOException, InterruptedException
        {
            StringBuilder line = new StringBuilder(contents.toString());
            for (int i = 0; i < line.length(); i++)
            {
                if (!Character.isLetter(line.charAt(i)))
                {
                    line.replace(i, i + 1, " ");
                }
            }
            String[] tokens = line.toString().split(" ");
            for (String s:tokens)
            {
                if (s.length() > 0)
                {
                    context.write(new Text(s), one);
                }
            }
        }
    }

    public class Reduce extends Reducer<Text, LongWritable, Text, Text>
    {
        public void reduce(Text key, Iterable<LongWritable> counts, Context context) throws IOException, InterruptedException
        {
            long sum = 0;
            for(LongWritable tmp:counts)
            {
                sum += tmp.get();
            }

            context.write(key, new Text(String.valueOf(sum)));
        }
    }

    public int run(String[] args) throws Exception
    {
        if(args.length != 2)
        {
            System.err.println("Usage: MoabLicenses <input> <output>");
            System.exit(-1);
        }

        Configuration conf = getConf();
        Job job = new Job(conf, "SrcTok");
        job.setJarByClass(CodeTokenizer.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);

        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception
    {
        GenericOptionsParser parse = new GenericOptionsParser(new Configuration(), args);
        Configuration conf = parse.getConfiguration();

        int res = ToolRunner.run(conf, new CodeTokenizer(),
                parse.getRemainingArgs());

        System.exit(res);
    }
}
