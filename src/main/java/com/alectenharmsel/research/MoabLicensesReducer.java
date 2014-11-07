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
        HashMap<String,Integer> sums = new HashMap<String,Integer>();
        HashMap<String,Integer> nums = new HashMap<String,Integer>();
        HashMap<String,Integer> tots = new HashMap<String,Integer>();

        for(Text tmp:counts)
        {
            String[] split = tmp.toString().split(",");

            if(sums.containsKey(split[0]))
            {
                int tmpSum = sums.get(split[0]);
                int tmpNum = nums.get(split[0]);
                int tmpTot = tots.get(split[0]);

                sums.remove(split[0]);
                nums.remove(split[0]);
                tots.remove(split[0]);

                tmpSum += Integer.parseInt(split[1]);
                tmpNum++;
                if(tmpTot < Integer.parseInt(split[2]))
                {
                    tmpTot = Integer.parseInt(split[2]);
                }

                sums.put(split[0], tmpSum);
                nums.put(split[0], tmpNum);
                tots.put(split[0], tmpTot);
            }
            else
            {
                int parsedSum = Integer.parseInt(split[1]);
                int parsedTot = Integer.parseInt(split[2]);
                sums.put(split[0], parsedSum);
                nums.put(split[0], 1);
                tots.put(split[0], parsedTot);
            }
        }

        String[] licenses = new String[sums.keySet().toArray().length];
        Integer[] rawSums = new Integer[sums.values().toArray().length];
        Integer[] rawNums = new Integer[nums.values().toArray().length];
        Integer[] rawTots = new Integer[tots.values().toArray().length];
        for(int i = 0; i < sums.keySet().toArray().length; i++)
        {
            try
            {
                licenses[i] = (String) sums.keySet().toArray()[i];
            }
            catch(ClassCastException e)
            {
                context.write(key, new Text("Caught a ClassCastException"));
            }
            catch(ArrayIndexOutOfBoundsException e)
            {
                context.write(key, new Text("licenses out of bounds: " + i + " out of " + licenses.length));
            }

            try
            {
                rawSums[i] = (Integer) sums.values().toArray()[i];
            }
            catch(ClassCastException e)
            {
                context.write(key, new Text("Caught a ClassCastException"));
            }
            catch(ArrayIndexOutOfBoundsException e)
            {
                context.write(key, new Text("sums out of bounds: " + i + " out of " + licenses.length));
            }

            try
            {
                rawNums[i] = (Integer) nums.values().toArray()[i];
            }
            catch(ClassCastException e)
            {
                context.write(key, new Text("Caught a ClassCastException"));
            }
            catch(ArrayIndexOutOfBoundsException e)
            {
                context.write(key, new Text("nums out of bounds: " + i + " out of " + licenses.length));
            }

            try
            {
                rawTots[i] = (Integer) tots.values().toArray()[i];
            }
            catch(ClassCastException e)
            {
                context.write(key, new Text("Caught a ClassCastException"));
            }
            catch(ArrayIndexOutOfBoundsException e)
            {
                context.write(key, new Text("tots out of bounds: " + i + " out of " + licenses.length));
            }
        }

        for(int i = 0; i < licenses.length; i++)
        {
            try
            {
                context.write(key, new Text(licenses[i] + "," + (rawSums[i].doubleValue() / rawNums[i].doubleValue()) + "," + rawTots[i]));
            }
            catch(ArrayIndexOutOfBoundsException e)
            {
                context.write(key, new Text("Final out of bounds: " + i + " out of " + licenses.length));
            }
        }
    }
}
