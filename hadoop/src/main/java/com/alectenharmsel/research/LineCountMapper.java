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
 * The mapper
 */

package com.alectenharmsel.research;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

//Takes in filename and file contents
public class LineCountMapper extends Mapper<Text, Text, Text, LongWritable>
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
