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

package com.alectenharmsel.research.spark;

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object LineCount {

  private var in = "";
  private var out = "";

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: LineCount <in> [out]")
      System.exit(1)
    }

    in = args(0)
    out = args(1)

    val conf = new SparkConf().setAppName("LineCount")
    val sc = new SparkContext(conf)
    val raw = sc.textFile(in)
    val sum = raw.map(line => 1).reduce((a, b) => a + b)

    System.out.println(sum)

    sc.stop()
  }
}
