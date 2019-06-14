# test for multicoreprocessing pool
# reference: https://www.machinelearningplus.com/python/parallel-processing-python/

import time
import numpy as np
import pandas as pd
import multiprocessing as mp
from multiprocessing.pool import ThreadPool, Pool
 
start = time.time()

def square_it(x):
    return x*x
 
print("Number of processors:", mp.cpu_count())


# On Windows, make sure that multiprocessing doesn't start
# until after "if __name__ == '__main__'" 
 
with Pool(processes=mp.cpu_count()) as pool:
   results = pool.map(square_it, [5, 4, 3, 2 ,1])
 
print(results) 

end = time.time()

print("time:", end-start)

# Parallelize a Pandas Dataframe
# row and column interation   = multprocessing
# the entire dateframe itself = pathos package (dill method) 

df = pd.DataFrame(np.random.randint(3, 10, size =[5, 2])) 
print("data for row wise iteration:", df)


# 1. row wise operation of pandas dataframe

def hypotenuse(row):
    return round(row[1]**2 + row[2]**2, 2)**0.5



with Pool(processes=mp.cpu_count()) as pool:
     result = pool.imap(hypotenuse, df.itertuples(name=False), chunksize =10) # itertuples passing row
     output = [round(x,2) for x in result]

print("row wise iteration output:", output)

# 2. column wise operation of pandas dataframe

def sum_of_squares(column):
    return sum([i**2 for i in column[1]])


with Pool(processes=mp.cpu_count()) as pool:
    result = pool.imap(sum_of_squares, df.iteritems(), chunksize=5)
    output2 =[x for x in result]

print("column wise iteration output:", output2)


# 3. entire dataframe interation
# requires pathos and dill

from pathos.multiprocessing import ProcessingPool as Pool

df2 =pd.DataFrame(np.random.randint(3, 10, size=[500,2]))

def func(df):
    return df.shape

cores = mp.cpu_count()


df_split = np.array_split(df, cores, axis =0) # split an array into sub-arrays (array, num of sub arrary)

# creative the multiprocessing pool

pool = Pool(cores)


# process the DataFrame by mapping unction to each df2 across the pool

df_out =np.vstack(pool.map(func, df_split)) # stack arrays in sequence verticlally (row1 row2...)


print("dataframe iteration:", df_out)


# close down the pool and join

pool.close()
pool.join()
pool.clear()





























# [25, 16, 9, 4, 1]


#



