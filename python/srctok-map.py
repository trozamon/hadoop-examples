#!/usr/bin/env python

import fileinput

tokens = dict()

for line in fileinput.input():
    tokenized = []
    for c in line:
        if c.isalpha():
            tokenized.append(c)
        else:
            tokenized.append(' ')

    tok = ''.join(tokenized).split(' ')
    for token in tok:
        if len(token) > 0:
            if token not in tokens.keys():
                tokens[token] = 1
            else:
                tokens[token] = tokens[token] + 1

for token in tokens.keys():
    print(token + '\t' + str(tokens[token]))
