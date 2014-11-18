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
 * Read the whole block into memory as text
 */

package com.alectenharmsel.research;

import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class WholeBlockInputFormat extends FileInputFormat<Text, Text>
{
    public WholeBlockRecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
    {
        FileSplit fileSplit = (FileSplit) split;
        return new WholeBlockRecordReader();
    }
}
