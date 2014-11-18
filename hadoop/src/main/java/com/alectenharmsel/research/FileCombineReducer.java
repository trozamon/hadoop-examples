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
 * FileCombineReducer
 *
 * Output everything
 */

package com.alectenharmsel.research;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class FileCombineReducer extends Reducer<Text, LongWritable, Text, LongWritable>
{
    public void reduce(Text key, Iterable<LongWritable> vals, Context context) throws IOException, InterruptedException
    {
        for(LongWritable tmp:vals)
        {
            context.write(key, tmp);
        }
    }
}
