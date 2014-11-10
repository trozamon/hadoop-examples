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
 * The reducer
 */

package com.alectenharmsel.research;

import java.io.IOException;
import java.util.Iterator;

import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;

public class MoabLicensesReducer extends Reducer<Text, Text, Text, Text>
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

        context.write(key, new Text(avgAvail + "," + avgTotal));
    }
}
