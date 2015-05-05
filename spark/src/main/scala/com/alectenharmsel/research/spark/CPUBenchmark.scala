/* 
 * Copyright 2015 Alec Ten Harmsel
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
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

object CPUBenchmark {

  def main(args: Array[String]) {
    if (args.length != 3) {
      println("Usage: CPUBenchmark <input> <min_k> <max_k>")
      return
    }

    /*
     * NOTE: Set spark.executor.memory if not submitting to YARN, otherwise
     * the default of 512MB is used.
     */
    val conf = new SparkConf().setAppName("CPUBenchmark")
    val sc = new SparkContext(conf)

    val min = args(1).toInt
    val max = args(2).toInt

    /*
     * NOTE: Call coalesce() on 'data' if not submitting to YARN, otherwise
     * it will split into way too many partitions
     */
    val rawData = sc.textFile(args(0))

    val data = rawData.map(
      line => Vectors.dense(line.split(',').map(_.toDouble))
    ).cache()

    for (k <- min to max)
    {
      val kmeans = new KMeans().setK(k).setMaxIterations(20)
      val clusters = kmeans.run(data)
      val wssse = clusters.computeCost(data)
      println(k + "," + wssse)
    }

    sc.stop()
  }

}

