# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""


import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
rdd=spark.sparkContext.parallelize([1,2,3,4,5])

rddCollect = rdd.collect()
print(f"Number of Partitions: {str(rdd.getNumPartitions())}")
print(f"Action: First element: {str(rdd.first())}")
print(rddCollect)

emptyRDD = spark.sparkContext.emptyRDD()
emptyRDD2 = rdd=spark.sparkContext.parallelize([])

print(f"{str(emptyRDD2.isEmpty())}")


