"""The main package for the Python Module Distribution Utilities.
A normal use is to load a dataframe in pyspark code. By loading
the dataframe this way, fixture testing of the notebook is enabled:

   from birgitta.dataframe import dataframe
   dataframe.get(spark_session, 'dataset_name')
"""
