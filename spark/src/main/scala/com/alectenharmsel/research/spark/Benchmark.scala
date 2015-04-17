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

import java.security.MessageDigest
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object Benchmark {

  var hasher = MessageDigest.getInstance("SHA-512")

  def main(args: Array[String]) {
    if (args.length != 1) {
      println("Usage: Benchmark <input>")
      return
    }

    /*
     * NOTE: Set spark.executor.memory if not submitting to YARN, otherwise
     * the default of 512MB is used.
     */
    val conf = new SparkConf().setAppName("Benchmark")
    val sc = new SparkContext(conf)

    /*
     * NOTE: Call coalesce() on 'data' if not submitting to YARN, otherwise
     * it will split into way too many partitions
     */
    val data = sc.textFile(args(0))
    val res = data.map(elem => hash(elem.toString))

    println("Hashed " + res.count().toString + " lines")

    sc.stop()
  }

  def hash(str: String): String = {
    hasher.reset()
    hasher.update(str.getBytes)
    return hasher.digest().toString
  }

}
