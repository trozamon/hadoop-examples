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
import org.apache.spark.rdd.RDD

object AverageNGramLength {

  private var in = "";
  private var out = "";

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: AverageNGramsLength <in> <out>")
      System.exit(1)
    }

    in = args(0)
    out = args(1)

    val conf = new SparkConf().setAppName("AverageNGramsLength")
    val sc = new SparkContext(conf)

    val ngrams = sc.textFile(in).map(line => line.split("\t"))

    val yearlyAvg = run(ngrams)

    yearlyAvg.saveAsTextFile(out)

    sc.stop()
  }

  def run(data: RDD[Array[String]]): RDD[(Int, Double)] = {
    val yearlyLengthAll = data.map(
      arr => (arr(1).toInt, arr(0).size.toDouble * arr(2).toDouble)
    )
    val yearlyLength = yearlyLengthAll.reduceByKey((a, b) => a + b)
    val yearlyCount = data.map(
      arr => (arr(1).toInt, arr(2).toDouble)
    ).reduceByKey((a, b) => a + b)
    val yearlyAvg = yearlyLength.join(yearlyCount).map(
      tup => (tup._1, tup._2._1 / tup._2._2)
    )

    return yearlyAvg
  }
}
