/* 
 * Copyright 2014 Alec Ten Harmsel
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

package com.alectenharmsel.research;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class SrcTokMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
    private final LongWritable one = new LongWritable(1);

    public void map(LongWritable key, Text contents, Mapper.Context context) throws IOException, InterruptedException
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
            context.write(new Text(s), one);
        }
    }
}
