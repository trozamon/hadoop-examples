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
 * RecordReader that returns the text in one HDFS block
 */

package com.alectenharmsel.research;

import java.io.IOException;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeBlockRecordReader extends RecordReader<Text, Text>
{
    private FileSplit fileSplit;
    private boolean processed = false;
    private Text currKey, currValue;
    private long start, fileLength;
    private int blockSize;
    private Configuration conf;

    public WholeBlockRecordReader()
    {

    }

    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
    {
        fileSplit = (FileSplit) split;
        conf = context.getConfiguration();
        //blockSize = conf.getInt("dfs.block.size", 134217728);
        //128MB seems too big
        blockSize = 1024 * 1024; //1MB Blocks
        //blockSize = 1024; //Testing ONLY
        fileLength = (int) fileSplit.getLength();

        currKey = createKey();
        currValue = createValue();
    }

    public Text createKey()
    {
        return new Text();
    }

    public Text createValue()
    {
        return new Text();
    }

    public float getProgress() throws IOException
    {
        return ((float) start ) / ((float) fileSplit.getLength());
    }

    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        if(!processed)
        {
            System.err.println("start is " + start);
            Path file = fileSplit.getPath();
            String tmp = file.toString();
            System.err.println("File: " + tmp);
            currKey.set(tmp);
            System.err.println("Reached this point");
            FileSystem fs = file.getFileSystem(conf);
            System.err.println("fs blocksize: " + fs.getDefaultBlockSize(file));
            System.err.println("linecount blocksize: " + blockSize);
            byte[] contents;
            FSDataInputStream in = null;
            try
            {
                in = fs.open(file);
                System.err.println("getPos(): " + in.getPos());

                if((start + blockSize) > fileLength)
                {
                    blockSize = (int) (fileLength - start);
                    processed = true;
                }
                
                contents = new byte[blockSize];

                //IOUtils.readFully(in, contents, start, blockSize);
                //IOUtils.readFully(in, contents, 0, blockSize);
                in.readFully(start, contents);

                start += blockSize;

                currValue.set(contents);
            }
            finally
            {
                IOUtils.closeStream(in);
            }
            return true;
        }
        return false;
    }

    public Text getCurrentKey() throws IOException, InterruptedException
    {
        return currKey;
    }

    public Text getCurrentValue() throws IOException, InterruptedException
    {
        return currValue;
    }

    public void close() throws IOException
    {
        return;
    }
}
