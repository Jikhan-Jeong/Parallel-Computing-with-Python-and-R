# -*- coding: utf-8 -*-
"""
Created on Fri May 10 13:45:01 2019

@author: Jikhan Jeong
"""
# 2019_05_10_multiprocessing
# reference: https://docs.python.org/ko/3/library/multiprocessing.html

# https://pypi.org/project/multiprocess/
# (install) pip install multiprocess

from multiprocessing import Pool

def f(x):
    return x*x

if __name__ == '__name__':
    with Pool(5) as p:
        print(p.map(f, [1, 2, 3]))
        
# 2. How many maximum parallel processes can you run?
        
import multiprocessing as mp
print("number of processors: ", mp.cpu_count())

# 3. What is Synchronous and Asynchronous execution?

# 3.1 Pool Class
# Synchronous execution
# Pool.map() and Pool.starmap()
# Pool.apply()

# 3.2 Asynchronous execution
# Pool.map_async() and Pool.starmap_async()
# Pool.apply_async())

# 4. Problem Statement: Count how many numbers exist between a given range in each row
import numpy as np
from time import time

# prepare data

np.random.RandomState(100)
arr = np.random.randint(0,10, size=[200000,5]) # randint low, high, size
data = arr.tolist()
data[:5]

# Solution without parallelization




