from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

sc= SparkContext()
spark = SparkSession.builder \
    .appName('example') \
    .master('local') \
    .getOrCreate()

data = [
    ('1', 'Sly', 'Blann', 'sblann0@about.com', 'Male', '234.12.183.196'), 
    ('2', 'Quintana', 'Martins', 'qmartins1@huffingtonpost.com', 'Genderqueer', '204.39.108.170'),
    ('3', 'Elsie', 'Calrow', 'ecalrow2@diigo.com', 'Female', '125.178.201.103'), 
    ('4', 'Cathy', 'Fulham', 'cfulham3@bigcartel.com', 'Female', '213.63.76.211'), 
    ('5', 'Vitoria', 'Duesberry', 'vduesberry4@people.com.cn', 'Male', '86.58.25.64'), 
    ('6', 'Zebedee', 'Joyce', 'zjoyce5@who.int', 'Male', '13.18.172.165'), 
    ('7', 'Viki', 'Snuggs', 'vsnuggs6@hatena.ne.jp', 'Male', '1.79.88.236'), 
    ('8', 'Kalli', 'Ygou', 'kygou7@wp.com', 'Female', '235.181.155.197'), 
    ('9', 'Mallory', 'Mayo', 'mmayo8@woothemes.com', 'Male', '18.117.189.187'), 
    ('10', 'Tera', 'Charrier', 'tcharrier9@yelp.com', 'Male', '95.223.81.249')
]

"""
Difference between RDD and DataFrame

RDD is a distributed collection od data elements spread across many machines in the cluster.
RDDs are a set of Java or Scala objects representing data.

DataFrame is a distributed collection of data organized into named columns.
"""

rdd = sc.parallelize(data)
df = spark.createDataFrame(data)

# conversion between rdd and df

# rdd2df
new_df = rdd.toDF(schema=None) # customized schema can be applied here
new_rdd = df.rdd 


# write parquet file from DataFrame
df.write.parquet('example.parquet')
# or
df.write.format('parquet').save('example.parquet')

# read parquet file from DataFrame
df.read.format('parquet').load('example.parquet')
# or
df.read.parquet('example.parquet')

# to overwrite an existing parquet
df.write.format('parquet') \
    .mode('overwrite') \
    .save('path_to_parquet')

