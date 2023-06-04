
import re

import requests
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, ArrayType


COUNTRY_LIST = {
    "Afghanistan": "AFG", "Åland Islands": "ALA", "Albania": "ALB", "Algeria": "DZA",
    "American Samoa": "ASM", "Andorra": "AND", "Angola": "AGO", "Anguilla": "AIA",
    "Antarctica": "ATA", "Antigua Barbuda": "ATG", "Argentina": "ARG",
    "Armenia": "ARM", "Aruba": "ABW", "Ascension Island": "NA", "Australia": "AUS",
    "Austria": "AUT", "Azerbaijan": "AZE", "Bahamas": "BHS", "Bahrain": "BHR",
    "Bangladesh": "BGD", "Barbados": "BRB", "Belarus": "BLR", "Belgium": "BEL",
    "Belize": "BLZ", "Benin": "BEN", "Bermuda": "BMU", "Bhutan": "BTN",
    "Bolivia": "BOL", "Bosnia Herzegovina": "BIH",
    "Botswana": "BWA", "Bouvet Island": "BVT", "Brazil": "BRA",
    "Britain": "GBR", "Great Britain": "GBR",
    "British Virgin Islands": "VGB", "Brunei": "BRN", "Bulgaria": "BGR", "Burkina Faso": "BFA",
    "Burundi": "BDI", "Cambodia": "KHM", "Cameroon": "CMR",
    "Canada": "CAN", "Cape Verde": "CPV", "Cayman Islands": "CYM",
    "Central African Republic": "CAF", "Chad": "TCD", "Chile": "CHL", "China": "CHN", "P.R.China": "CHN", "P. R. China": "CHN", " P. R. China": "CHN",
    "Cocos Islands": "CCK", "Colombia": "COL", "People's Republic of China": "CHN", "The Netherlands": "NLD",
    "Comoros": "COM",     "Republic of Congo": "COG", "Cook Islands": "COK",
    "Costa Rica": "CRI", "Cote Ivoire": "CIV", "Ivory Coast": "CIV", "Croatia": "HRV", "Cuba": "CUB",
    "Curaçao": "CUW", "Cyprus": "CYP", "Czech Republic": "CZE", "Denmark": "DNK",
    "Djibouti": "DJI", "Dominica": "DMA", "Dominican Republic": "DOM", "Democratic Republic of Congo": "COD",
    "Ecuador": "ECU", "Egypt": "EGY", "El Salvador": "SLV", "England": "GBR",
    "Equatorial Guinea": "GNQ", "Eritrea": "ERI", "Estonia": "EST", "Ethiopia": "ETH",
    "Falkland Islands": "FLK", "Faroe Islands": "FRO",
    "Fiji": "FJI", "Finland": "FIN", "France": "FRA", "French Guiana": "GUF",
    "French Polynesia": "PYF", "Gabon": "GAB",
    "Gambia": "GMB", "Georgia": "GEO", "Germany": "DEU", "Ghana": "GHA",
    "Gibraltar": "GIB", "Greece": "GRC", "Greenland": "GRL", "Grenada": "GRD",
    "Guadeloupe": "GLP", "Guam": "GUM", "Guatemala": "GTM", "Guernsey": "GGY",
    "Guinea": "GIN", "Guinea Bissau": "GNB", "Guyana": "GUY", "Haiti": "HTI", "Honduras": "HND",
    "Hong Kong": "HKG",  "Hungary": "HUN", "Iceland": "ISL",
    "India": "IND", "Indonesia": "IDN", "Iran": "IRN", "Iraq": "IRQ", "Ireland": "IRL",
    "Israel": "ISR", "Italy": "ITA", "Jamaica": "JAM", "Japan": "JPN",
    "Jordan": "JOR", "Kazakhstan": "KAZ", "Kenya": "KEN",
    "Kiribati": "KIR", "Kosovo": "XKX", "Kuwait": "KWT", "Kyrgyzstan": "KGZ", "Laos": "LAO",
    "Latvia": "LVA", "Lebanon": "LBN", "Lesotho": "LSO", "Liberia": "LBR",
    "Libya": "LBY", "Liechtenstein": "LIE", "Lithuania": "LTU", "Luxembourg": "LUX",
    "Macau": "MAC", "Macedonia": "MKD", "Madagascar": "MDG", "Malawi": "MWI",
    "Malaysia": "MYS", "Maldives": "MDV", "Mali": "MLI", "Malta": "MLT", "Marshall Islands": "MHL",
    "Martinique": "MTQ", "Mauritania": "MRT", "Mauritius": "MUS",
    "Mayotte": "MYT", "Mexico": "MEX", "Micronesia": "FSM", "Moldova": "MDA",
    "Monaco": "MCO", "Mongolia": "MNG", "Montenegro": "MNE", "Montserrat": "MSR",
    "Morocco": "MAR", "Mozambique": "MOZ", "Myanmar": "MMR", "Burma": "MMR", "Namibia": "NAM",
    "Nauru": "NRU", "Nepal": "NPL", "Netherlands": "NLD", "Netherlands Antilles": "ANT",
    "New Caledonia": "NCL", "New Zealand": "NZL", "Nicaragua": "NIC",
    "Niger": "NER", "Nigeria": "NGA", "Niue": "NIU", "North Korea": "PRK",
    "Northern Ireland": "IRL", "Northern Mariana Islands": "MNP",
    "Norway": "NOR", "Oman": "OMN", "Pakistan": "PAK",
    "Palau": "PLW", "Palestine": "PSE", "Panama": "PAN", "Papua New Guinea": "PNG",
    "Paraguay": "PRY", "Peru": "PER", "Philippines": "PHL", "Pitcairn Islands": "PCN",
    "Poland": "POL", "Portugal": "PRT", "Puerto Rico": "PRI",
    "Qatar": "QAT", "Réunion": "REU", "Romania": "ROU", "Russia": "RUS",
    "Rwanda": "RWA", "Saint Barthélemy": "BLM", "Saint Helena": "SHN",
    "Saint Kitts Nevis": "KNA", "Saint Lucia": "LCA",
    "Saint Pierre Miquelon": "SPM", "Saint Vincent Grenadines": "VCT",
    "Samoa": "WSM", "San Marino": "SMR", "São Tomé Príncipe": "STP", "Saudi Arabia": "SAU",
    "Senegal": "SEN", "Serbia": "SRB",
    "Seychelles": "SYC", "Sierra Leone": "SLE", "Singapore": "SGP", "Sint Maarten": "SXM",
    "Slovakia": "SVK", "Slovenia": "SVN", "Solomon Islands": "SLB",
    "Somalia": "SOM", "South Africa": "ZAF", "Korea": "KOR",
    "South Korea": "KOR", "South Sudan": "SSD", "Spain": "ESP", "Sri Lanka": "LKA", "Sudan": "SDN",
    "Suriname": "SUR", "Svalbard Jan Mayen": "SJM",
    "Swaziland": "SWZ", "Sweden": "SWE", "Switzerland": "CHE", "Syria": "SYR",
    "Taiwan": "TWN", "Tajikistan": "TJK", "Tanzania": "TZA", "Thailand": "THA",
    "Timor Leste": "TLS", "East Timor": "TLS", "Togo": "TGO", "Tokelau": "TKL", "Tonga": "TON", "Trinidad Tobago": "TTO",
    "Tunisia": "TUN", "Turkey": "TUR",
    "Turkmenistan": "TKM", "Turks Caicos Islands": "TCA", "Tuvalu": "TUV", "U.S. Minor Outlying Islands": "UMI",
    "Virgin Islands": "VIR", "Uganda": "UGA",
    "Ukraine": "UKR", "United Arab Emirates": "ARE", "United Kingdom": "GBR",
    "United States": "USA",    "Uruguay": "URY", "Uzbekistan": "UZB", "Vanuatu": "VUT", "Vatican": "VAT",
    "Venezuela": "VEN", "ROC": "TWN",
    "Vietnam": "VNM", "Wallis Futuna": "WLF",
    "Western Sahara": "ESH", "Yemen": "YEM", "Zambia": "ZMB", "Zimbabwe": "ZWE",
    "UK": "GBR", "USA": "USA", "America": "USA",  "Palestinian Territories": "PSE",
    "Congo Brazzaville": "COG", "DRC": "COD", "Congo Kinshasa": "COD", "Wales": "GBR",
    "Scotland": "GBR"
}


USA_STATE_LIST = {
    'AL': 'Alabama',
    'AK': 'Alaska',
    'AZ': 'Arizona',
    'AR': 'Arkansas',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DE': 'Delaware',
    'FL': 'Florida',
    'GA': 'Georgia',
    'HI': 'Hawaii',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'IA': 'Iowa',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'ME': 'Maine',
    'MD': 'Maryland',
    'MA': 'Massachusetts',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MS': 'Mississippi',
    'MO': 'Missouri',
    'MT': 'Montana',
    'NE': 'Nebraska',
    'NV': 'Nevada',
    'NH': 'New Hampshire',
    'NJ': 'New Jersey',
    'NM': 'New Mexico',
    'NY': 'New York',
    'NC': 'North Carolina',
    'ND': 'North Dakota',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'RI': 'Rhode Island',
    'SC': 'South Carolina',
    'SD': 'South Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VT': 'Vermont',
    'VA': 'Virginia',
    'WA': 'Washington',
    'WV': 'West Virginia',
    'WI': 'Wisconsin',
    'WY': 'Wyoming',
    'DC': 'District of Columbia',
    'MP': 'Northern Mariana Islands',
    'PW': 'Palau',
    'PR': 'Puerto Rico',
    'VI': 'Virgin Islands',
    'AA': 'Armed Forces Americas (Except Canada)',
    'AE': 'Armed Forces Africa/Canada/Europe/Middle East',
    'AP': 'Armed Forces Pacific'
}



spark = SparkSession.builder.appName('query').getOrCreate()
# spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
df = spark.read.parquet("../data/gep_entity")


test_df = df.limit(20000)



def structure_results(res):
    """Format Elasticsearch result as Python dictionary"""
    out = {'hits': {'hits': []}}
    keys = ['admin1_code', 'admin2_code', 'admin3_code', 'admin4_code',
            'alternativenames', 'asciiname',  'coordinates',
            'country_code2', 'country_code3',
            'feature_class', 'feature_code', 'geonameid',
            'modification_date', 'name', 'population']
    for i in res:
        i_out = {}
        for k in keys:
            i_out[k] = i[k]
        out['hits']['hits'].append(i_out)
    return out


def get_asciiname_countrycode(out_res):
    """get the asciiname from result"""
    try:
        return out_res['hits']['hits'][0]["asciiname"], out_res['hits']['hits'][0]['country_code3']
    except IndexError:
        return None


def get_place_res(placename, country, S):
    """base on the placename to query the corresponding cities"""

    if placename == "" or placename is None or placename == '':
        return None
    q = {"multi_match": {"query": placename, "fields": [
        'name^5', 'asciiname^5', 'alternativenames'], "operator": "and"}}
    # res = S.filter("term", feature_code="PCLI").query(q)[0:5].execute()
    # if res is not None:
    #     print(res)
    # try:
    if country:
        res = S.query(q)[0].execute()
    else:
        res = S.filter("term", country_code3=country).query(q)[0].execute()

    if res:
        return get_asciiname_countrycode(structure_results(res))
    return None

def http_search(input_string):
    """search in osm"""
    url = 'http://nominatim.openstreetmap.org/search'
    params = {'q': input_string, 'format': 'json',
              'addressdetails': 1, 'limit': 1, 'accept-language': 'en'}
    r = requests.get(url, params=params, timeout=60)
    results = r.json()
    try:
        print(results[0]['address']['state'], results[0]['address']['country'])
        return results[0]['address']['state'], results[0]['address']['country']
    except(IndexError, AttributeError, KeyError):
        return None


def spark_map(input_s, S):
    """"get the entity place name based on the input array"""

    # S = 'hh'
    if len(input_s["gepEntities"]) == 0:
        return None, None
    if len(input_s["gepEntities"]) == 1:
        if input_s["gepEntities"][-1] in COUNTRY_LIST:
            return input_s['gepEntities'], [COUNTRY_LIST[input_s["gepEntities"][-1]]]
        tmp_res = get_place_res(input_s["gepEntities"][-1], '', S)
        return input_s['gepEntities'], tmp_res

    if input_s["gepEntities"][-1] in COUNTRY_LIST:
        if COUNTRY_LIST[input_s["gepEntities"][-1]] == 'USA' and input_s["gepEntities"][-2] in USA_STATE_LIST:
            return input_s['gepEntities'], [USA_STATE_LIST[input_s["gepEntities"][-2]], 'USA']

        city = re.sub(r'[0-9]+', '',
                        input_s["gepEntities"][-2])
        tmp_res = get_place_res(city,
                                COUNTRY_LIST[input_s["gepEntities"][-1]], S)
        return input_s['gepEntities'], tmp_res
    if input_s["gepEntities"][-1] in USA_STATE_LIST:
        # tmp_res = get_place_res(input_s["gepEntities"][-2], 'USA')
        return input_s['gepEntities'], [USA_STATE_LIST[input_s["gepEntities"][-1]], 'USA']

    tmp_res = get_place_res(input_s["gepEntities"][-1], '', S)
    return input_s['gepEntities'], tmp_res


def partitins_map(iterator):
    """map functions with partitions"""
    kwargs = {"hosts": ['localhost'], "port": 9200, "use_ssl": False}
    CLIENT = Elasticsearch(**kwargs)
    S = Search(using=CLIENT, index="geonames")
    res = []
    for row in iterator:
        res.append(spark_map(row, S))
    yield res


gep_type = StructType([
    # Define a StructField for each field
    StructField('instFull',  ArrayType(StringType()), True),
    StructField('gep', ArrayType(StringType()), True),
])

final_df = df.rdd.repartition(4).mapPartitions(
    partitins_map).saveAsTextFile('./gep_test')

