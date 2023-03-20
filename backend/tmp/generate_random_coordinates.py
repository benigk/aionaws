import random


"""
The data format:

geojsonFeature = {
    "type": "Feature",
    "properties": {
        "name": "Gothenburg University",
        "city": "Gothenburg",
        "country": "Sweden",
        "totalNumberOfPublications": 100,
        "numberOfPublicationsByYear:[
            {
                "year": 2000,
                "numberOfPublications": 40,
            },
            {
                "year": 2010,
                "numberOfPublications": 60,
            }
        ],
        "geometry": {
            "type": "Point",
            "coordinates":[float(), float()]
        }
    }
}
"""

import json
import random

def read_json(file_name):
    with open(file_name, mode="r", encoding="utf-8") as f:
        return json.load(f)


def generate_coordinates():
    def generate_long():
        return random.randint(-180, 180) + round(random.random(), 5)
    def generate_lat():
        return random.randint(-90, 90) + round(random.random(), 5)
    return [generate_long(), generate_lat()]

def normalize(input_list, n):
    s = sum(input_list)
    
    distribution_list = [int(e/s*n) for e in input_list[:-1]]
    return distribution_list + [s - sum(distribution_list)]
    
def generate_publication_number(min_num=1, max_num=100000):

    num_years = random.randint(1,120)
    n = random.randint(max(min_num, num_years), max_num)
    years = sorted(random.sample(list(range(1900, 2022)), num_years))
    ratio_by_year = [random.random() for _ in range(num_years)]
    number_by_year = normalize(ratio_by_year, n)
    
    publication_year = zip(years, number_by_year)
    return n, publication_year

def get_country_list():
    countries = read_json('countries.json')
    return [country.get('name') for country in countries]

def get_city_list():
    cities = read_json('cities.json')
    return [city.get('name') for city in cities]

def get_university_list():
    universities = read_json('universities.json')
    return [university.get('name') for university in universities]


def generate(countries, cities, universities):
    paper_num, publication_by_year = generate_publication_number()
    publication_by_year = [ {
                                'year':year, 
                                'numberOfPublications': num
                            } for year, num in list(publication_by_year) ]
    return {
        "type": "Feature",
        "properties": {
            "name": random.choices(universities)[0],
            "city": random.choices(cities)[0],
            "country": random.choices(countries)[0],
            "totalNumberOfPublications": paper_num,
            "numberOfPublicationsByYear": publication_by_year,
            "geometry": {
                "type": "Point",
                "coordinates": generate_coordinates()
            }
        }
    }


def main():
    # countries = get_country_list()
    # cities = get_city_list()
    # universities = get_university_list()
    with open('fake_coordinates.json', 'w') as f:
        f.write('[')
        f.write('\n')
        for i in range(1000000):
            print(i)
            f.write(f'\t{{"coordinates": {generate_coordinates()}}}')
            f.write(',\n')
        f.write(']\n')
    # with open('fake_coordinates', 'a+') as f:
    #     for i in range(10):
    #         print(i)
            # instance = generate(countries, cities, universities)
            # json.dump(instance, f, indent=4)


if __name__ == '__main__':
    main()

