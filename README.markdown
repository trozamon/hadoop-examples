# Hadoop Examples/Testing

This code is tested against and used to verify a CDH 5.3.2 cluster.

This repo contains:

* A collection of example jobs:
  * Hadoop
  * Hadoop Streaming
  * Spark
  * PySpark
  * Hive
  * Pig
  * MRJob
* A script to run all or some of these in an automated way to ensure a properly
  operating cluster

## Cluster Testing

    python/util/cluster_test.py

The above script will upload data and run sample jobs to ensure a properly
configured cluster.

## Building

    mvn package

The above command will build and package all of the Java and Scala code, as
well as run MapReduce/Spark unit tests to ensure correctness.
