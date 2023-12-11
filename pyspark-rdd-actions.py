# -*- coding: utf-8 -*-
"""
Created on Sun Jun 14 10:20:19 2020
"""


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data=[("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)]
inputRDD = spark.sparkContext.parallelize(data)

listRdd = spark.sparkContext.parallelize([1,2,3,4,5,3,2])

#aggregate
seqOp = (lambda x, y: x + y)
combOp = (lambda x, y: x + y)
agg=listRdd.aggregate(0, seqOp, combOp)
print(agg) # output 20

#aggregate 2
seqOp2 = (lambda x, y: (x[0] + y, x[1] + 1))
combOp2 = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
agg2=listRdd.aggregate((0, 0), seqOp2, combOp2)
print(agg2) # output (20,7)

agg2=listRdd.treeAggregate(0,seqOp, combOp)
print(agg2) # output 20

#fold
from operator import add
foldRes=listRdd.fold(0, add)
print(foldRes) # output 20

#reduce
redRes=listRdd.reduce(add)
print(redRes) # output 20

#treeReduce. This is similar to reduce
add = lambda x, y: x + y
redRes=listRdd.treeReduce(add)
print(redRes) # output 20

#Collect
data = listRdd.collect()
print(data)

#count, countApprox, countApproxDistinct
print(f"Count : {str(listRdd.count())}")
#Output: Count : 20
print(f"countApprox : {str(listRdd.countApprox(1200))}")
#Output: countApprox : (final: [7.000, 7.000])
print(f"countApproxDistinct : {str(listRdd.countApproxDistinct())}")
#Output: countApproxDistinct : 5
print(f"countApproxDistinct : {str(inputRDD.countApproxDistinct())}")
#Output: countApproxDistinct : 5

#countByValue, countByValueApprox
print(f"countByValue :  {str(listRdd.countByValue())}")


#first
print(f"first :  {str(listRdd.first())}")
#Output: first :  1
print(f"first :  {str(inputRDD.first())}")
#Output: first :  (Z,1)

#top
print(f"top : {str(listRdd.top(2))}")
#Output: take : 5,4
print(f"top : {str(inputRDD.top(2))}")
#Output: take : (Z,1),(C,40)

#min
print(f"min :  {str(listRdd.min())}")
#Output: min :  1
print(f"min :  {str(inputRDD.min())}")
#Output: min :  (A,20)  

#max
print(f"max :  {str(listRdd.max())}")
#Output: max :  5
print(f"max :  {str(inputRDD.max())}")
#Output: max :  (Z,1)

#take, takeOrdered, takeSample
print(f"take : {str(listRdd.take(2))}")
#Output: take : 1,2
print(f"takeOrdered : {str(listRdd.takeOrdered(2))}")
#Output: takeOrdered : 1,2
print(f"take : {str(listRdd.takeSample())}")


