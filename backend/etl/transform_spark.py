"""transform the location to the country code and coordinates"""

import json
import re
import subprocess
from datetime import datetime

import pycountry
import requests
from mordecai import Geoparser
from pyspark import SparkFiles
from pyspark.sql import SparkSession


def extract_location(input_string):
    """simple extraction without parsing location"""
    res = []
    locations = input_string.split(",")
    i = -1
    # get rid of email address"
    country = re.sub(
        r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", "", locations[-1])
    country = re.sub(r"Electronic address: ", "", country)
    # get rid of space and .
    country = country.rstrip(".").strip().rstrip(".").strip()
    if country:
        res.append(country)
    else:
        try:
            country = locations[-2]
            country = country.rstrip(".").strip().rstrip(".").strip()
            res.append(country)
            i = -2
        except IndexError:
            return None, None, None

    if len(locations) > 3:
        for location in reversed(locations[i-2:i]):
            res.append(location)
        return res[0], res[1], res[2]

    for i in reversed(range(len(locations[:i]))):
        res.append(locations[i])
    while len(res) < 3:
        res.append(None)
    return res[0], res[1], res[2]


def install_util():
    """install utils for slaves"""
    with open(SparkFiles.get("install.sh"), encoding="UTF-8") as testFile:
        for line in testFile.readlines():
            subprocess.call(line, shell=True)
    yield print("success")


def read_parquet(iterator):
    """main function, read parquet and store in country_json and write it to json file
    """
    # print(inst["firstInst"])
    # print(inst)
    # try:
    # yield sum(iterator)
    geo = Geoparser(threads=False)
    country_dict = {}
    country_in_json = {}

    def extract_coordinates(input_string):
        """parse the location and return corresponding coordinates"""
        location = geo.geoparse(input_string)
        try:  # pylint: disable=too-many-try-statements
            if len(location) == 0 or location[0]["country_conf"] < 0.6:
                url = "http://nominatim.openstreetmap.org/search"
                params = {"q": input_string, "format": "json",
                          "addressdetails": 1, "limit": 1, "accept-language": "en"}
                r = requests.get(url, params=params, timeout=60)
                results = r.json()
                country_code = pycountry.countries.get(
                    alpha_2=results[0]["address"]["country_code"]).alpha_3
                latitude = results[0]["lat"]
                longitude = results[0]["lon"]
            else:
                country_code = location[0]["country_predicted"]
                latitude = location[0]["geo"]["lat"]
                longitude = location[0]["geo"]["lon"]
            return (longitude, latitude, country_code)

        except (IndexError, AttributeError, ValueError):
            # logging.error(input_string)
            # logging.error(e)
            return (None, None, None)

    for row in iterator:
        loc = extract_location(row["firstInst"])

        try:  # pylint: disable=too-many-try-statements
            if loc is None or loc[0] is None:
                continue
            if loc[0] not in country_dict:
                coo = extract_coordinates(loc[0])
                if coo[-1] not in country_in_json:
                    country_in_json[coo[-1]] = {}
                    country_in_json[coo[-1]]["location"] = coo[:-1]
                    country_in_json[coo[-1]]["account"] = 1
                else:
                    country_in_json[coo[-1]]["account"] += 1
            else:
                country_in_json[country_dict[loc[0]]]["account"] += 1
        except (IndexError, TypeError, ValueError):
            # logging.error(row["firstInst"])
            pass

    yield country_in_json


def json_to_geojson():
    """change format function, covert country json file to geojson format"""
    with open("country.json", encoding="UTF-8") as json_file:
        countrys = json.load(json_file)

    with open("../../frontend/public/countries.geo.json", encoding="UTF-8") as json_file:
        frontend_countrys = json.load(json_file)

    for feature in frontend_countrys["features"]:
        if feature["id"] in countrys.keys():
            feature["properties"]["account"] = countrys[feature["id"]]["account"]
        else:
            feature["properties"]["account"] = 0

    with open("geojson.json", "w", encoding="UTF-8") as geojson_file:
        json.dump(frontend_countrys, geojson_file, indent=4)


if __name__ == "__main__":
    spark = SparkSession.builder.appName("test").getOrCreate()

    # df = spark.read.parquet("s3a://quokka-2021-thesis/pubmed21-part-extraced/")
    df = spark.read.parquet("../data/600k.parquet")
    spark.sparkContext.addFile("./install.sh")

    df_valid = df.filter(df.validity is True)

    df_inst = df_valid.select("firstInst")

    print(datetime.now().strftime("%H:%M:%S"))
    country_json = df_inst.rdd.repartition(
        2).mapPartitions(read_parquet).collect()
    print(datetime.now().strftime("%H:%M:%S"))

    with open("./testjson.json", "w", encoding="UTF-8") as f:
        json.dump(country_json, f, indent=4)
