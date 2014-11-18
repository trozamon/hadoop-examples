package com.alectenharmsel.research.spark

import java.io.IOException
import java.util.ArrayList
import java.util.List
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.scalatest._

class MoabLicenseInfoTest extends FlatSpec with Matchers {

  val conf = new SparkConf().setMaster("local").setAppName("test")
  val sc = new SparkContext(conf)
  val data = sc.parallelize(
    Array[String](
      "05/11 22:58:25  MNodeUpdateResExpression(nyx5624,FALSE,TRUE)",
      "05/11 22:58:25  INFO:     License cfd_solv_ser        0 of   6 available  (Idle: 33.3%  Active: 66.67%)",
      "05/11 22:59:25  INFO:     License cfd_solv_ser        0 of   6 available  (Idle: 33.3%  Active: 66.67%)",
      "05/11 23:58:25  INFO:     License cfd_solv_ser        1 of   7 available  (Idle: 33.3%  Active: 66.67%)",
      "05/11 23:59:25  INFO:     License cfd_solv_ser        0 of   7 available  (Idle: 33.3%  Active: 66.67%)"
    )
  )
  val res = MoabLicenseInfo.run(data).collect()

  it should "return a single result" in {
    res.size should be (1)
  }

  it should "have 1 available" in {
    res(0)._3 should be (1)
  }

  it should "have 26 total" in {
    res(0)._4 should be (26)
  }

  it should "have a proper date" in {
    res(0)._1 should be ("05/11")
  }

  it should "have a proper license name" in {
    res(0)._2 should be ("cfd_solv_ser")
  }
}
