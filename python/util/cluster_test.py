import sys
import argparse
import subprocess
import os
from random import seed, randint

seed()

queue = 'staff'
__version_hadoop__ = '2.5.0-cdh5.3.2'
__version_spark__ = '1.2.0-cdh5.3.2'
prefix = 'trozamon_testing' + str(randint(0, 100000))
input_unstructured = '/'.join([prefix, 'input_unstructured'])
input_structured = '/'.join([prefix, 'input_structured'])
output_prefix = '/'.join([prefix, 'output'])
mrjob_conf = 'python/mrjob.conf'

big_description = """Hadoop and related software cluster tester. Given the
fast-paced nature of Hadoop, Spark, Hive, and other technologies, running a
test suite after an upgrade can ensure that all expected functionality
continues to work.  This script will generate and upload data to
$HOME/trozamon_testing in HDFS."""

def run_cmd(cmd):
    print(' '.join(['Running:', cmd]))
    return subprocess.call(cmd, shell=True)

def hdfs_mkdir(dir):
    if run_cmd('hdfs dfs -test -d ' + dir) != 0:
        if run_cmd('hdfs dfs -mkdir ' + dir) != 0:
            print('Failed to create ' + dir + ' in HDFS')
            return 1
    return 0

def hdfs_put(local, remote):
    if run_cmd('hdfs dfs -test -f ' + remote) != 0:
        if run_cmd(' '.join(['hdfs dfs -put', local, remote])) != 0:
            print('Failed to upload ' + local + ' to ' + remote)
            return 1
    return 0

def hdfs_rmdir(dir):
    if run_cmd('hdfs dfs -test -d ' + dir) == 0:
        return run_cmd('hdfs dfs -rm -r ' + dir)
    return 0

def test_hadoop_java():
    outdir = '/'.join([output_prefix, 'hadoop_java'])
    cmd = ' '.join([
        'yarn jar hadoop/target/hadoop-examples-hadoop-' + __version_hadoop__ + '.jar',
        'com.alectenharmsel.research.LineCount',
        '-Dmapreduce.job.queuename=' + queue,
        input_unstructured,
        outdir
        ])

    if hdfs_rmdir(outdir) != 0:
        return 1

    return run_cmd(cmd)

def test_mrjob():
    cmd = ' '.join([
        'python2.7',
        'python/mrjob_test.py',
        'structured.data',
        '-r',
        'hadoop'
        ])

    return run_cmd(cmd)

def test_hadoop_streaming():
    outdir = '/'.join([output_prefix, 'hadoop_streaming'])
    cmd = ' '.join([
        'yarn jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar',
        '-Dmapreduce.job.queuename=' + queue,
        '-input ' + input_unstructured,
        '-output ' + outdir,
        '-mapper python/srctok-map.py -reducer python/sum.py',
        '-file python/srctok-map.py -file python/sum.py'
        ])

    if hdfs_rmdir(outdir) != 0:
        return 1

    return run_cmd(cmd)

def test_pig():
    cmd = ' '.join([
        'pig',
        '-Dmapreduce.job.queuename=' + queue,
        '-f pig/cluster_test.pig'
        ])

    return run_cmd(cmd)

def test_spark():
    outdir = '/'.join([output_prefix, 'spark'])
    cmd = ' '.join([
        'spark-submit',
        '--master yarn-client',
        '--queue ' + queue,
        '--class com.alectenharmsel.research.spark.LineCount',
        'spark/target/hadoop-examples-spark-' + __version_spark__ + '.jar',
        input_unstructured,
        outdir
        ])

    if hdfs_rmdir(outdir) != 0:
        return 1

    return run_cmd(cmd)

def test_pyspark():
    cmd = ' '.join([
        'spark-submit',
        '--master yarn-client',
        '--queue ' + queue,
        'python/spark/lc.py',
        input_unstructured
        ])

    return run_cmd(cmd)

def test_hive():
    cmd = ' '.join([
        'hive',
        '--hiveconf mapreduce.job.queuename=' + queue,
        '-f hive/cluster_test.sql'
        ])

    return run_cmd(cmd)

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
    parser.add_argument('--mrjob',
            action='store_true',
            help='Run a mrjob script')

    parsed_args = parser.parse_args()

    tests = list()
    if parsed_args.all:
        parsed_args.hadoop_java = True
        parsed_args.hadoop_streaming = True
        parsed_args.pig = True
        parsed_args.hive = True
        parsed_args.spark = True
        parsed_args.pyspark = True
        parsed_args.mrjob = True

    if parsed_args.hadoop_java:
        tests.append(test_hadoop_java)
    if parsed_args.hadoop_streaming:
        tests.append(test_hadoop_streaming)
    if parsed_args.pig:
        tests.append(test_pig)
    if parsed_args.pyspark:
        tests.append(test_pyspark)
    if parsed_args.spark:
        tests.append(test_spark)
    if parsed_args.mrjob:
        os.environ['MRJOB_CONF'] = '/'.join([os.environ['PWD'], mrjob_conf])
        tests.append(test_mrjob)
    # Hive should always be last - it munges data
    if parsed_args.hive:
        tests.append(test_hive)

    if run_cmd('mvn package -DskipTests') != 0:
        print('Maven build failed. Are you running this in the root ' +
                'directory of the repo?')
        return 1

    print('Uploading data...')
    err = hdfs_mkdir(prefix)
    if err != 0:
        return err
    err = hdfs_mkdir(input_unstructured)
    if err != 0:
        return err
    err = hdfs_mkdir(input_structured)
    if err != 0:
        return err
    err = hdfs_mkdir(output_prefix)
    if err != 0:
        return err
    err = hdfs_put('pom.xml', '/'.join([input_unstructured, 'pom.xml']))
    if err != 0:
        return err
    err = hdfs_put('structured.data', '/'.join([input_structured, 'data']))
    if err != 0:
        return err

    print('Starting to run tests...')
    results = {}
    ret = 0
    for test in tests:
        res = test()
        if res == 0:
            results[test.__name__] = True
        else:
            ret = 1
            results[test.__name__] = False

    for test in sorted(results.keys()):
        if results[test]:
            print(test + ': SUCCESS')
        else:
            print(test + ': FAILURE')

    return ret

if __name__ == '__main__':
    sys.exit(run())
