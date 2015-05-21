/* 
 * Copyright 2015 Alec Ten Harmsel
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
 * A BAM summary similar to `samtools flagstat`
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
import org.seqdoop.hadoop_bam.BAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import htsjdk.samtools.SAMRecord;

public class BAMSummary extends Configured implements Tool
{
    public static String getRecordName(String longName)
    {
        String[] name = longName.split("\\.");
        if (name.length == 0)
        {
            return "unnamed";
        }

        return name[0];
    }

    public static class Map extends
        Mapper<LongWritable, SAMRecordWritable, Text, LongWritable>
    {
        public void map(LongWritable key, SAMRecordWritable contents,
                Context context) throws IOException,
               InterruptedException
        {
            SAMRecord rec = contents.get();
            Boolean proper = false;
            String name = getRecordName(rec.getReadName());
            LongWritable one = new LongWritable(1);

            try
            {
                proper = rec.getProperPairFlag();

                if (proper)
                {
                        context.write(new Text(name + "-properlypaired"), one);
                }

                if (rec.getSecondOfPairFlag())
                {
                    context.write(new Text(name + "-read2"), one);
                }
                else
                {
                    context.write(new Text(name + "-read1"), one);
                }
            }
            catch (IllegalStateException e)
            {
            }

            if (rec.getDuplicateReadFlag())
            {
                context.write(new Text(name + "-duplicates"), one);
            }

            if (rec.isValid() != null)
            {
                context.write(new Text(name + "-invalid"), one);
            }

            context.write(new Text(name + "-total"), one);
        }
    }

    public static class Reduce extends
            Reducer<Text, LongWritable, Text, LongWritable>
    {
        public void reduce(Text key, Iterable<LongWritable> vals,
                        Context context) throws IOException,
               InterruptedException
        {
            long sum = 0;

            for(LongWritable tmp : vals)
            {
                sum += tmp.get();
            }

            context.write(key, new LongWritable(sum));
        }
    }

    public int run(String[] args) throws Exception
    {
        if(args.length != 2)
        {
            System.err.println("Usage: BAMSummary <input> <output>");
            System.exit(-1);
        }

        Job job = Job.getInstance(getConf(), "BAMSummary");
        job.setJarByClass(BAMSummary.class);

        job.setInputFormatClass(BAMInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception
    {
        GenericOptionsParser parse = new GenericOptionsParser(
                        new Configuration(), args);
        Configuration conf = parse.getConfiguration();

        int res = ToolRunner.run(conf, new BAMSummary(),
                        parse.getRemainingArgs());

        System.exit(res);
    }
}
