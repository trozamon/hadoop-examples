import random
import argparse

INTMAX = 1000

parser = argparse.ArgumentParser()
parser.add_argument('n', metavar='n', help='number of points')
args = parser.parse_args()

out = list()

random.seed()

for i in range(0, int(args.n)):
    print('/'.join([str(i), args.n]))

    line = [str(random.randint(0, INTMAX)), str(random.randint(0, INTMAX))]

    out = out + [','.join(line)]

with open('kmeans_data.txt', 'w') as f:
    f.write("\n".join(out))
