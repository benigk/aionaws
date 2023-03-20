"""transform the location to the country code and coordinates"""
import csv
import logging
import json
import re
import pycountry
import requests
from pyspark import SparkContext
from pyspark.sql import SparkSession
from mordecai import Geoparser


def extract_location(input_string):
    """simple extraction without parsing location"""
    res = []
    locations = input_string.split(',')
    i = -1
    # get rid of email address"
    country = re.sub(
        r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '', locations[-1])
    country = re.sub(r'Electronic address: ', '', country)
    # get rid of space and .
    country = country.rstrip('.').strip().rstrip('.').strip()
    if country:
        res.append(country)
    else:
        try:
            country = locations[-2]
            country = country.rstrip('.').strip().rstrip('.').strip()
            res.append(country)
            i = -2
        except IndexError:
            return None, None, None

    if len(locations) > 3:
        for location in reversed(locations[i-2:i]):
            res.append(location)
        return res[0], res[1], res[2]
    else:
        for i in reversed(range(len(locations[:i]))):
            res.append(locations[i])
        while len(res) < 3:
            res.append(None)
        return res[0], res[1], res[2]


geo = Geoparser()

# store all the paired 'extract country name' and corresponding '3 digit country code'
country_dict = {}
# Based on the country code store the coordinates and account
country_json = {}
# store all the locations extraction
locations = []


def extract_coordinates(input_string, level=""):
    """parse the location and return corresponding coordinates"""
    location = geo.geoparse(input_string)
    try:
        if len(location) == 0 or location[0]['country_conf'] < 0.6:
            url = 'http://nominatim.openstreetmap.org/search'
            params = {'q': input_string, 'format': 'json',
                      'addressdetails': 1, 'limit': 1, 'accept-language': 'en'}
            r = requests.get(url, params=params)
            results = r.json()
            country_code = pycountry.countries.get(
                alpha_2=results[0]['address']['country_code']).alpha_3
            latitude = results[0]['lat']
            longitude = results[0]['lon']
        else:
            country_code = location[0]['country_predicted']
            latitude = location[0]['geo']['lat']
            longitude = location[0]['geo']['lon']
        return longitude, latitude, country_code

    except (IndexError, AttributeError):
        logging.error(input_string)
        return None, None, None


def read_parquet():
    """
       main function, read parquet and store in country_json and write it to json file
    """
    logging.basicConfig(filename='example.log',
                        filemode='w', level=logging.ERROR)
    sc = SparkContext()
    spark = SparkSession(sc)

    df = spark.read.parquet("../data/600k.parquet")

    df_valid = df.filter(df.validity == True)

    df_inst = list(df_valid.select(
        'firstInst').collect())

    for inst in df_inst:
        try:
            loc = extract_location(inst['firstInst'])

            if loc[0] not in country_dict:
                coo = extract_coordinates(loc[0])
                country_dict[loc[0]] = coo[-1]
                if coo[-1] not in country_json:
                    country_json[coo[-1]] = {}
                    country_json[coo[-1]]["location"] = coo[:-1]
                    country_json[coo[-1]]["account"] = 1
                else:
                    country_json[coo[-1]]["account"] += 1
                locations.append([inst['firstInst'], coo[-1], loc[1], loc[2]])
            else:
                country_json[country_dict[loc[0]]]["account"] += 1
                locations.append(
                    [inst['firstInst'], country_dict[loc[0]], loc[1], loc[2]])

        except (IndexError, TypeError):
            logging.error(inst['firstInst'])

    with open("country.json", 'w', encoding='UTF-8',) as f:
        json.dump(country_json, f, indent=4)


def json_to_geojson():
    """
          change format function, covert country json file to geojson format
    """
    with open('country.json', encoding='UTF-8') as json_file:
        countrys = json.load(json_file)

    with open('../../frontend/public/countries.geo.json', encoding='UTF-8') as json_file:
        frontend_countrys = json.load(json_file)

    for feature in frontend_countrys['features']:
        if feature['id'] in countrys.keys():
            feature['properties']['account'] = countrys[feature['id']]['account']
        else:
            feature['properties']['account'] = 0

    with open("geojson.json", 'w', encoding='UTF-8') as f:
        json.dump(frontend_countrys, f, indent=4)


def generate_test_dataset():
    """
          change format function, covert country json file to geojson format
    """

    with open("test.csv", "w", encoding='UTF-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["row_data", "extract_country", "extract_city", "extract_affiliation",
                         "expected_country", "expected_city", "expected_affiliation"])

        for loc in locations:
            writer.writerow(loc)


if __name__ == "__main__":
    read_parquet()
    # # generate_test_dataset()
    # json_to_geojson()
