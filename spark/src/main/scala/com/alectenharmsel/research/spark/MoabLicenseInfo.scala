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

package com.alectenharmsel.research.spark;

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object MoabLicenseInfo {

  private var in = "";
  private var out = "";

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: AverageNGramsLength <in> <out>")
      System.exit(1)
    }

    in = args(0)
    out = args(1)

    val conf = new SparkConf().setAppName("MoabLicenseInfo")
    val sc = new SparkContext(conf)

    val raw = sc.textFile(in)

    val licenseData = run(raw)

    licenseData.saveAsTextFile(out)

    sc.stop()
  }

  def run(data: RDD[String]): RDD[(String, String, Double, Double)] = {
    val licensesRaw = data.filter(line => line.contains("License"))

    val split = licensesRaw.map(
      line => line.split(" ")
    ).map(
      arr => arr.filter(x => x.size >= 1)
    ).filter(
      arr => arr.size >= 8
    )

    val licenses = split.map(
      arr => Array[String](
        arr(4) + "-" + arr(0).replaceAll("/", "-"),
        arr(5),
        arr(7)
      )
    )

    val sum = licenses.map(
      arr => (arr(0), arr(1).toDouble)
    ).reduceByKey((a, b) => a + b)

    val total = licenses.map(
      arr => (arr(0), arr(2).toDouble)
    ).reduceByKey((a, b) => a + b)

    val ret = sum.join(total).map(
      tup => (
        tup._1.split("-").tail.mkString("/"),
        tup._1.split("-")(0),
        tup._2._1,
        tup._2._2
      )
    )

    return ret
  }
}
