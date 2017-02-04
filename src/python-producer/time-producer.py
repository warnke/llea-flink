#!/usr/bin/env python
## printing to stderr in python3 style
from __future__ import print_function
import sys

import logging, time

#import numpy as np

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

count = 0
while True:
    tt = time.time()
    count = count + 1 if count < 3 else 1
    print("currtime-{}".format(count), '{:.2f}'.format(time.time()))
    time.sleep(1)

