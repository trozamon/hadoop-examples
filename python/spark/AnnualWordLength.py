from pyspark import SparkConf, SparkContext
import sys

if len(sys.argv) != 3:
    print('Usage: ' + sys.argv[0] + ' <in> <out>')
    sys.exit(1)

input = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('AnnualWordLength')
sc = SparkContext(conf=conf)

data = sc.textFile(input).map(lambda line: line.split('\t'))

yearlyLengthAll = data.map(
    lambda arr: (int(arr[1]), float(len(arr[0])) * float(arr[2]))
)

yearlyLength = yearlyLengthAll.reduceByKey(lambda a, b: a + b)

yearlyCount = data.map(
    lambda arr: (int(arr[1]), float(arr[2]))
).reduceByKey(
    lambda a, b: a + b
)

yearlyAvg = yearlyLength.join(yearlyCount).map(
    lambda tup: (tup[0], tup[1][0] / tup[1][1])
)

yearlyAvg.saveAsTextFile(output)

sc.stop()
