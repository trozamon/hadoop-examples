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
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.Configuration;

public class MoabLicensesMapper extends Mapper<LongWritable, Text, Text, Text>
{
    public void map(LongWritable key, Text contents, Context context) throws IOException, InterruptedException
    {
        if(contents.toString().contains("License"))
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
