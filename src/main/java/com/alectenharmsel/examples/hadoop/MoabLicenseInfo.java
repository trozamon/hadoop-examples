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

package com.alectenharmsel.examples.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MoabLicenseInfo
{
    public static class Map extends Mapper<LongWritable, Text, Text, Text>
    {
        public void map(LongWritable key, Text contents, Context context) throws IOException, InterruptedException
        {
            if (contents.toString().contains("License"))
            {
                String date = "";
                String licenseInfo = "";
                String pkgName = "";
                ArrayList<String> license = new ArrayList<String>();
                String[] blah = contents.toString().split(" ");

                for(String tmp:blah)
                {
                    if(tmp.length() != 0)
                    {
                        license.add(tmp);
                    }
                }

                if (license.size() != 13)
                {
                    return;
                }

                date = license.get(0).replaceAll("/", "-");
                pkgName = license.get(4);
                licenseInfo += license.get(5) + "," + license.get(7);
                context.write(new Text(pkgName + "-" + date), new Text(licenseInfo));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> counts, Context context) throws IOException, InterruptedException
        {
            int sum = 0;
            int num = 0;
            int total = 0;

            for(Text tmp:counts)
            {
                String[] split = tmp.toString().split(",");
                sum += Integer.parseInt(split[0]);
                total += Integer.parseInt(split[1]);
                num++;
            }

            double avgAvail = (double)sum / (double) num;
            String avgTotal = "";
            if (total % num == 0) {
                avgTotal = String.valueOf(total/num);
            } else {
                avgTotal = String.valueOf((double) total / (double) num);
            }

            String[] keyArr = key.toString().split("-");
            String keyOut = keyArr[keyArr.length - 2] + "-" +
                keyArr[keyArr.length - 1];

            keyOut += ",";
            for (int i = 0; i < keyArr.length - 2; i++) {
                if (i > 0) {
                    keyOut += "-";
                }
                keyOut += keyArr[i];
            }

            context.write(new Text(keyOut), new Text(avgAvail + "," + avgTotal));
        }
    }

    public static void main(String[] args) throws Exception
    {
        GenericOptionsParser parser = new GenericOptionsParser(new Configuration(), args);
        Configuration conf = parser.getConfiguration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        String[] remainingArgs = parser.getRemainingArgs();
        if(remainingArgs.length != 2)
        {
            System.err.println("Usage: LineCount <input> <output>");
            System.exit(-1);
        }

        Job job = Job.getInstance(conf, "MoabLicenseInfo");
        job.setJarByClass(MoabLicenseInfo.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

        int res = job.waitForCompletion(true) ? 0 : 1;
        System.exit(res);
    }
}
