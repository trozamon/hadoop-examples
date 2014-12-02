import sys
import argparse
import subprocess

big_description = """Hadoop and related software cluster tester. Given the
fast-paced nature of Hadoop, Spark, Hive, and other technologies, running a
test suite after an upgrade can ensure that all expected functionality
continues to work.  This script will generate and upload data to
$HOME/trozamon_testing in HDFS."""

def test_hadoop_java():
    print("Testing Hadoop Java")
    return 0

def test_hadoop_streaming():
    print("Testing Hadoop Streaming")
    return 0

def test_pig():
    print("Testing Pig")
    return 0

def test_spark():
    print("Testing Spark")
    return 0

def test_pyspark():
    print("Testing PySpark")
    return 0

def test_hive():
    print("Testing Hive")
    return 0

def run():
    parser = argparse.ArgumentParser(description=big_description)

    parser.add_argument('--all',
            action='store_true',
            help='Run all codes that would be run by using all possible flags')
    parser.add_argument('--hadoop-java',
            action='store_true',
            help='Run a selection of Java codes written for Hadoop')
    parser.add_argument('--hadoop-streaming',
            action='store_true',
            help='Run a selection of Python codes written for Hadoop')
    parser.add_argument('--pig',
            action='store_true',
            help='Run a selection of Pig scripts')
    parser.add_argument('--spark',
            action='store_true',
            help='Run a selection of Scala codes written for Spark')
    parser.add_argument('--pyspark',
            action='store_true',
            help='Run a selection of Python codes written for Spark')
    parser.add_argument('--hive',
            action='store_true',
            help='Run a selection of Hive queries')

    parsed_args = parser.parse_args()

    tests = list()
    if parsed_args.all:
        parsed_args.hadoop_java = True
        parsed_args.hadoop_streaming = True
        parsed_args.pig = True
        parsed_args.hive = True
        parsed_args.spark = True
        parsed_args.pyspark = True

    if parsed_args.hadoop_java:
        tests.append(test_hadoop_java)
    if parsed_args.hadoop_streaming:
        tests.append(test_hadoop_streaming)
    if parsed_args.pig:
        tests.append(test_pig)
    if parsed_args.hive:
        tests.append(test_hive)
    if parsed_args.pyspark:
        tests.append(test_pyspark)
    if parsed_args.spark:
        tests.append(test_spark)

    print("Running \"mvn package -DskipTests\"...")
    if subprocess.call("mvn package -DskipTests", shell=True,
            stdout=subprocess.DEVNULL) != 0:
        print("Maven build failed. Are you running this in the root " +
                "directory of the repo?")
        return 1

    print("Uploading pom.xml to trozamon_testing/input/pom.xml...")
    if subprocess.call("hdfs dfs -mkdir trozamon_testing", shell=True) != 0:
        return 1
    if subprocess.call("hdfs dfs -mkdir trozamon_testing/input",
            shell=True) != 0:
        return 1
    if subprocess.call(
            "hdfs dfs -put pom.xml trozamon_testing/input/pom.xml",
            shell=True,
            stdout=subprocess.DEVNULL) != 0:
        print("Uploading files to HDFS is _not_ working")
        return 1

    print("Starting to run tests...")
    for test in tests:
        res = test()
        if res != 0:
            print("FAILURE")
            return res

    return 0

if __name__ == "__main__":
    sys.exit(run())
