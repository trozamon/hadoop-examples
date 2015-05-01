import random

DIM = 5000
out = list()

random.seed()

for i in range(0, DIM):
    print('/'.join([str(i), str(DIM)]))

    line = list()
    for j in range(0, DIM):
        line = line + [str(random.randint(0, 1000000))]

    out = out + [' '.join(line)]

with open('kmeans_data.txt', 'w') as f:
    f.write("\n".join(out))
