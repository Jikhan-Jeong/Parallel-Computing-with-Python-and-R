# Parallel-Computing-with-Python-and-R

Speed up! Scale Up!  

* **Dask**  
import dask.dataframe as dd  
from dask.diagnostics import ProgressBar  
pbar = ProgressBar()  
pbar.register()  
  
* Ref: https://www.machinelearningplus.com/python/parallel-processing-python/
* Ref: https://docs.dask.org/en/latest/install.html (conda install dask)
* Ref; https://datascienceschool.net/view-notebook/2282b75b2a63448087b77269885c27cb/ (Korean)
* Ref: https://towardsdatascience.com/trying-out-dask-dataframes-in-python-for-fast-data-analysis-in-parallel-aa960c18a915 (English)


---------------------  
* **CuDF**  
* Ref: https://github.com/rapidsai/cudf  
---------------------  
conda install -c rapidsai -c nvidia -c numba -c conda-forge cudf=0.10 python=3.7 cudatoolkit=10.0  

conda install -c nvidia/label/cuda10.0 -c rapidsai/label/cuda10.0 -c numba -c conda-forge -c defaults cudf  

* Ref: https://rapidsai.github.io/projects/cudf/en/0.10.0/10min.html  

