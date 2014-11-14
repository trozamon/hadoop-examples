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

package com.alectenharmsel.hadoop;

import com.alectenharmsel.research.WholeBlockInputFormat;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

enum LcCounters
{
    NUM_LINES
}

public class LineCount extends Configured implements Tool
{
    public class Map extends Mapper<Text, Text, Text, LongWritable>
    {
        public void map(Text key, Text contents, Context context) throws IOException, InterruptedException
        {
            long numLines = 0;
            String tmp = contents.toString();

            for(int i = 0; i < tmp.length(); i++)
            {
                if(tmp.charAt(i) == '\n')
                {
                    numLines++;
                }
            }

            context.write(key, new LongWritable(numLines));
        }
    }

    public class Reduce extends Reducer<Text, LongWritable, Text, LongWritable>
    {
        public void reduce(Text key, Iterable<LongWritable> counts, Context context) throws IOException, InterruptedException
        {
            long total = 0;

            for(LongWritable tmp:counts)
            {
                total += tmp.get();
            }

            context.getCounter(LcCounters.NUM_LINES).increment(total);
            context.write(key, new LongWritable(total));
        }
    }

    public int run(String[] args) throws Exception
    {
        if(args.length != 2)
        {
            System.err.println("Usage: LineCounter <input> <output>");
            System.exit(-1);
        }

        Job job = new Job(getConf(), "LineCount");
        job.setJarByClass(LineCount.class);

        job.setInputFormatClass(WholeBlockInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        Configuration check = job.getConfiguration();
        boolean success = job.waitForCompletion(true);

        //Get the counter here, output to a file called total in the dir
        Counters counters = job.getCounters();

        //Throw it in the file
        Path outPath = new Path(args[1]);
        FileSystem fs = outPath.getFileSystem(check);
        OutputStream out = fs.create(new Path(outPath, "total"));
        String total = counters.findCounter(LcCounters.NUM_LINES).getValue() + "\n";
        out.write(total.getBytes());
        out.close();
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception
    {
        GenericOptionsParser parse = new GenericOptionsParser(new Configuration(), args);
        Configuration conf = parse.getConfiguration();

        int res = ToolRunner.run(conf, new LineCount(), parse.getRemainingArgs());

        System.exit(res);
    }
}
