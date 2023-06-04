"""
This script is to convert data from parquet to generate data in format of json to be displayed online
input

Data structure of json file

{
    "countryCode": string,
    "paperCount": int,
    "articles": [
        {
            "pmid": string,
            "title": string,
            "journal": string,
            "year": string,
            "month": string,
            "day": string,
            "authors"
        }
    ]
}
"""
import argparse
import json
from collections import Counter

import country_converter as coco
from pyspark import SparkContext
from pyspark.sql import SparkSession


parser = argparse.ArgumentParser()
parser.add_argument("-i", "--input_dir", dest="input_path",
                    help="Path to filename")
parser.add_argument("-o", "--output_dir", dest="output_path",
                    default=".", help="Path of output directory")
parser.add_argument("-f", "--output_filename", dest="output_filename",
                    default="output.json", help="Output file name")

args = parser.parse_args()


# Start a spark session
sc = SparkContext()
spark = SparkSession(sc)

df = spark.read.parquet(args.input_path)

df_validity = df.filter(df.validity is True)

data_countries = [e for row in df_validity.select(
    "country").collect() for e in list(row)]

# get the institutions of the first author
data_inst = list(df_validity.select("firstInst").collect())


c = Counter(data_countries)
country_list = list(c.keys())


# convert country name to country code
name2iso2 = dict(zip(country_list, coco.convert(names=country_list, to="ISO2")))

data = {
    "countryData": {
        name2iso2.get(k, k): {
            "countryName": k,
            "paperCount": c.get(k)
        } for k in c
    }
}

data_location = [{"institution": e[0]} for e in data_inst]


# output data location
with open(args.output_filename, "w", encoding="utf-8") as f:
    json.dump(data_location, f, indent=4)

# output data_country
with open(args.output_filename, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4)
