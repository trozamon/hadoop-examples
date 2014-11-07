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

//Takes in filename and file contents
public class MoabLicensesMapper extends Mapper<LongWritable, Text, Text, Text>
{
    public String year;

    public void setup(Mapper.Context context)
    {
        year = context.getConfiguration().get("moab.year");
    }

    public void map(LongWritable key, Text contents, Context context) throws IOException, InterruptedException
    {

        String date = "";
        String licenseInfo = "";

        if(contents.toString().contains("License"))
        {
            ArrayList<String> license = new ArrayList<String>();
            String[] blah = contents.toString().split(" ");

            for(String tmp:blah)
            {
                if(tmp.length() != 0)
                {
                    license.add(tmp);
                }
            }

            date = license.get(0);

            for(int i = 2; i < license.size(); i++)
            {
                if(i == 4 || i == 5 || i == 7)
                {
                    licenseInfo += license.get(i) + ",";
                }

            }
            
            if(licenseInfo.length() > 2)
            {
                if(licenseInfo.endsWith(","))
                {
                    licenseInfo = licenseInfo.substring(0, licenseInfo.length() - 1);
                }
                //First arg is date string, second is license string
                context.write(new Text(date + "/" + year), new Text(licenseInfo));
            }
        }
    }
}
