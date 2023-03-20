#! /home/rex/pyspark/bin/python 

try:
    import pyspark
except ImportError:
    import findspark
    findspark.init('/home/rex/spark-3.2.0-bin-hadoop3.2')

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

import xml.etree.ElementTree as ET
import argparse
import gzip
import io
import os
import json
import argparse
from typing import DefaultDict
import requests
from bs4 import BeautifulSoup


sc = SparkContext()

PUBMED_URL = "https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
parser = argparse.ArgumentParser()
parser.add_argument("-o", "--output_dir", dest="output_path", default=".", help="Output path where your json file saved")
parser.add_argument("-s", "--size", dest="size", default=5, help="The number of files to be downloaed")
parser.add_argument("-e", "--extention", dest="ext",  default="gz", help="The format of downloaded files")
parser.add_argument("-f", "--output_filename", dest="file_name", default="pubmedData.json", help="The output filename")

args = parser.parse_args()


def get_filepaths(ext='gz', size=10):
    response = requests.get(url=PUBMED_URL, allow_redirects=True)
    soup = BeautifulSoup(response.text, 'html.parser')
    file_names = [a['href'] for a in soup.find_all('a', href=True, text=True) if a.get('href', None)]
    file_paths = [os.path.join(PUBMED_URL, file_name) for file_name in file_names if file_name.endswith(ext)]
    if size:
        file_paths = file_paths[:size]
    return file_paths
        

def ungzip(file_name):
    with gzip.open(file_name, 'rb') as f:
        return f.read()

def bytes2str(b, format="utf-8"):
    return b.decode(format)

def str2xml(string):
    return ET.fromstring(string)


def _parse_single_child_in_element(e):
    try:
        if e.text.strip():
            return e
    except Exception:
        return e        
    else:
        if len(list(e)) == 1:
            child = list(e)[0]
            return _parse_single_child_in_element(child)
    return e

def month_format(s):
    d = {
          text:number for text, number in zip(
            'jan feb mar apr may jun jul aug sep oct nov dec'.split(),
            '01 02 03 04 05 06 07 08 09 10 11 12'.split()
          ) 
    }
    converted_s = d.get(s.lower()[:3], None)
    if converted_s:
        return converted_s
    return s
    
def json_dump(cls):
    def to_json(self):
        return json.dumps(self, 
                          default=lambda o: o.__dict__,
                          sort_keys=True,
                          indent=4)
    cls.to_json = to_json
    return cls


@json_dump
class DateElement:
    """
    year
    month
    day
    """
    unprocessed_tags = DefaultDict(int)
    def __init__(self, date):
        self.year, self.month, self.day = None, None, None
        for child in list(date):
            lowered_tag = child.tag.lower()
            if lowered_tag == 'year':
                self.year = child.text
            elif lowered_tag == 'month':
                self.month = month_format(child.text)
            elif lowered_tag == 'day':
                self.day = child.text
            elif lowered_tag == 'hour':
                self.hour = child.text
            elif lowered_tag == 'minute':
                self.minute = child.text
            else:
                self.unprocessed_tags[child.tag] += 1
    
    def __str__(self):
        try:
            return '%s-%s-%s' % (self.year, self.month, self.day)
        except Exception:
            print('The date is not detected')
            return

@json_dump
class JournalIssue:
    """
    Volume
    Issue
    PubDate
    """
    unprocessed_tags = DefaultDict(int)
    def __init__(self, ji): #journalIssue
        self.volume, self.issue = None, None
        for child in list(ji):
            lowered_tag = child.tag.lower()
            if lowered_tag == 'volume':
                self.volume = child.text
            elif lowered_tag == 'issue':
                self.issue = child.text
            elif lowered_tag == 'pubdate':
                self.pubDate = DateElement(child)
            else:
                self.unprocessed_tags[child.tag] += 1
    
    def __str__(self):
        try:
            return "%s,%s$%s" % (self.volume, self.issue, str(self.pubDate))
        except Exception:
            return "The JournalIssue Element is not correctly formated. Volume: %s; Issue: %s!" % (self.volume, self.issue)

@json_dump
class Journal:
    """
    The following properties can be found within Jounal element
    ISSN,
    JournalIssue,
    Title,
    ISOAbbreviation
    """
    unprocessed_tags = DefaultDict(int)
    def __init__(self, journal):
        self.issn, self.title, self.isoabbreviation = None, None, None
        for child in list(journal):
            lowered_tag = child.tag.lower()
            if lowered_tag == 'issn':
                self.issn = child.text
                self.issnType = child.get('IssnType', '')
            elif lowered_tag == 'journalissue':
                self.journalIssue = JournalIssue(child)
            elif lowered_tag == 'title':
                self.title = child.text
            elif lowered_tag == 'isoabbreviation':
                self.isoabbreviation = child.text
            else:
                self.unprocessed_tags[child.tag] += 1


    def __str__(self):
        try:
            return '%s$%s$%s,%s' % (self.issn, str(self.journalIssue), self.title, self.isoabbreviation)   
        except Exception:
            return 'The Jounal Element is not correctly formated.' 

@json_dump
class Affiliation:
    affiliation_dict = DefaultDict(int)
    
    def __init__(self, affiliation):
        self.affiliation = affiliation.text
        self.affiliation_dict[self.affiliation] += 1

@json_dump
class Author:
    author_list = []
    unprocessed_tags = DefaultDict(int)
    def __init__(self, author):
        self.lastName, self.forename, self.initials = None, None, None
        for child in list(author):
            lowered_tag = child.tag.lower()
            if lowered_tag == 'lastname':
                self.lastName = child.text
            elif lowered_tag == 'forename':
                self.forename = child.text
            elif lowered_tag == 'initials':
                self.initials = child.text
            elif lowered_tag == 'affiliationinfo':
                self.affilitionList = [Affiliation(c) for c in list(child)]
            else:
                self.unprocessed_tags[child.tag] += 1
        self.author_list.append(self.__str__)
    def __str__(self):
        return '%s %s' % (self.lastName, self.forename)

@json_dump
class PublicationType:
    publication_type_dict = DefaultDict(list)
    
    def __init__(self, publicationTypeElement):
        self.publicationType = publicationTypeElement.text
    
    def __str__(self):
        return self.publicationType

@json_dump
class Article:
    """
    Journal Element
    ArticleTitle
    Pagination
    Abstract
    AuthorList
    Language
    PublicationTypeList
    VernacularTitle
    """
    unprocessed_tags = DefaultDict(int)
    def __init__(self, article):
        self.articleTitle, self.pagination, \
        self.abstract, self.language, \
        self.vernacularTitle = None, None, None, None, None
        
        for child in list(article):
            lowered_tag = child.tag.lower()
            if lowered_tag == 'journal':
                self.journal = Journal(child)
            elif lowered_tag == 'articletitle':
                self.articleTitle = child.text
            elif lowered_tag == 'pagination':
                self.pagination = _parse_single_child_in_element(child).text
            elif lowered_tag == 'abstract':
                self.abstract = _parse_single_child_in_element(child).text
            elif lowered_tag == 'authorlist':
                self.authorList = [Author(c) for c in list(child)]
            elif lowered_tag == 'language':
                self.language = child.text
            elif lowered_tag == 'publicationtypelist':
                self.publicationTypeList = [PublicationType(p) for p in list(child)]
            elif lowered_tag == 'vernaculartitle':
                self.vernacularTitle = child.text
            else:
                self.unprocessed_tags[child.tag] += 1
            
    def __str__(self):
        "!We may modify this str representation"
        return self.articleTitle

@json_dump
class MedlineCitation:
    unprocessed_tags = DefaultDict(int)

    def __init__(self, citation):
        self.PMID, self.citationSubset, self.coiStatement = None, None, None
        for child in list(citation):
            lowered_tag = child.tag.lower()
            if lowered_tag ==  'pmid':
                self.PMID = child.text
            elif lowered_tag == 'daterevised':
                self.dateRevised = DateElement(child)
            elif lowered_tag == 'article':
                self.article = Article(child)
            elif lowered_tag == 'citationsubset':
                self.citationSubset = child.text
            elif lowered_tag == 'coistatement':
                self.coiStatement = child.text
            else:
                self.unprocessed_tags[child.tag] += 1

@json_dump
class PubmedData:
    """
    history
    publicationStatus
    articleIdList
    """
    unprocessed_tags = DefaultDict(int)
    def __init__(self, pubmedData):
        self.history, self.publicationStatus, self.articleIdList = None, None, None
        for child in list(pubmedData):
            lowered_tag = child.tag.lower()
            if lowered_tag == 'history':
                self.history = [DateElement(c) for c in list(child)]
            elif lowered_tag == 'publicationstatus':
                self.publicationStatus = child.text
            elif lowered_tag == 'articleidlist':
                self.articleIdList = [(c.text) for c in list(child)]
            else:
                self.unprocessed_tags[child.tag] += 1

@json_dump
class PubmedArticle:
    unprocessed_tags = DefaultDict(int)
    def __init__(self, article):
        self.PMID, self.medlinecitation, self.pubmedData = None, None, None
        for child in list(article):
            lowered_tag = child.tag.lower()
            if lowered_tag == 'medlinecitation':
                self.medlinecitation = MedlineCitation(child)
                self.PMID = self.medlinecitation.PMID
            elif lowered_tag == 'pubmeddata':
                self.pubmedData = PubmedData(child)
            else:
                self.unprocessed_tags[child.tag] += 1


def pipeline(fp, file_name):
    ungziped_file = ungzip(os.path.join(fp, file_name))
    file_in_string = bytes2str(ungziped_file)
    xml_file = str2xml(file_in_string)
    articles = [PubmedArticle(child) for child in list(xml_file)]
    return articles


def parse():
    
    file_paths = get_filepaths(ext='gz', size=args.size)
    with open(os.path.join(args.output_path, args.file_name), 'w', encoding='utf-8') as output_file:
        for f in file_paths:
            print('processing %s' % f.split('/')[-1])
            response = requests.get(url=f)
            contentInBytes = gzip.decompress(response.content)
            contentInString = bytes2str(contentInBytes)
            contentInXml = str2xml(contentInString)

            for child in list(contentInXml):
                pubmedArticle = PubmedArticle(child)
                output_file.write(pubmedArticle.to_json())


# if __name__ == '__main__':
    
#    parse()



