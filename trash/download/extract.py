#! /home/rex/thesis/bin/python

import gzip
import os
import xml.etree.ElementTree as ET
import boto3

from parse import PubmedArticle



# scenario 1: we use boto 3 to fetch data from aws s3
s3 = boto3.client('s3',
                   region_name='eu-north-1',
                   aws_access_key_id='',
                   aws_secret_access_key=''
                   )

response = s3.list_buckets()



# scenario 2: we use downloaded file in our local device

def unzip(filename):
    with gzip.open(filename, 'rb') as f:
        return f.read()
    
def str2xml(string):
    return ET.fromstring(string)




from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local") \
    .appName("Pubmed") \
    .config() \
    .getOrCreate()
    
