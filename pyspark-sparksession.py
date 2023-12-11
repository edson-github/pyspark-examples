# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""


import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('SparkByExamples.com') \
                    .getOrCreate()

print("First SparkContext:");
print(f"APP Name :{spark.sparkContext.appName}");
print(f"Master :{spark.sparkContext.master}");

sparkSession2 = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExample-test") \
      .getOrCreate();

print("Second SparkContext:")
print(f"APP Name :{sparkSession2.sparkContext.appName}");
print(f"Master :{sparkSession2.sparkContext.master}");


sparkSession3 = SparkSession.newSession

print("Second SparkContext:")
print(f"APP Name :{sparkSession3.sparkContext.appName}");
print(f"Master :{sparkSession3.sparkContext.master}");