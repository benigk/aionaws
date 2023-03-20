# This is a test script to run jobs on aws

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

sc = SparkContext()

import gzip
import io

def extract(gz_file):
    [name, file] = gz_file
    print(name)
    # print(file)
    file_obj = gzip.GzipFile(fileobj=io.BytesIO(file), mode='r')
    line = file_obj.readline()
    record_count = 0
    line_buf = []
    recording = False
    while len(line) > 0:
        if line != None and b'<PubmedArticle>' in line:
            # record_count += 1
            recording = True
        if line != None and b'</PubmedArticle>' in line:
            # record_count += 1
            line_buf.append(line)
            curr_record = b''.join(line_buf)
            recording = False
            line_buf = []
            yield curr_record
        if recording:
            line_buf.append(line)
            # yield record_count
        line = file_obj.readline()
    # with gzip.GzipFile(fileobj=io.BytesIO(file), mode='r') as file_obj:
    #     line = file_obj.readline()
    #     while (line):
    #         if (line.find('<PubmedArticle>')):
    #             record_count += 1
    # return record_count

def test_sample(count):
    records = sc.binaryFiles('sample-data') \
        .flatMap(extract) \
        .take(count)
    for record in records:
        print(record)

from xml.etree.ElementTree import fromstring

def parse_article(article_record):

    root = fromstring(article_record)

    pmid = root.find('MedlineCitation/PMID').text
    # for pmid in root.iterfind('MedlineCitation/PMID'):
    #     print("- pmid: {}".format(pmid.text))

    def nodeChildText(node, childName:str):
        childNode = node.find(childName)
        if childNode != None:
            return childNode.text
        else:
            return None

    article = root.find('MedlineCitation/Article')
    article_title = article.find('ArticleTitle').text
    # print("- article title: {}".format(article_title))
    journal_title = article.find('Journal/Title').text
    # print("- journal title: {}".format(journal_title))
    authors = []
    for author in article.iterfind('AuthorList/Author'):
        # print(author)
        lastname = nodeChildText(author, 'LastName')
        forename = nodeChildText(author, 'ForeName')
        initials = nodeChildText(author, 'Initials')

        affiliations = []
        for affiliation in author.iterfind("AffiliationInfo/Affiliation"):
            affiliations.append(affiliation.text)
        author = {
            "lastname": lastname,
            "forename": forename,
            "initials": initials,
            "affiliations": affiliations
        }
        authors.append(author)

    return {
        "pmid": pmid,
        "title": article_title,
        "journal_title": journal_title,
        "authors": authors
    }

def test_extract_parse(path: str, save_path: str, count: int):
    articles = sc.binaryFiles(path) \
        .flatMap(extract) \
        .repartition(16) \
        .map(parse_article)

    print("article #: {}".format(articles.count()))
    # print("\n".join(samples))
    articles.saveAsTextFile(save_path)
    # samples = articles.take(count)
    # for article in samples:
    #     print(article)

s3_path = "s3a://quokka-2021-thesis/pubmed21-part/"
save_path = "s3a://quokka-2021-thesis/pubmed21-part-extraced/"
test_extract_parse(s3_path, save_path, 50)

sc.stop()