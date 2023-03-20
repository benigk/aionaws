"""System module"""
import argparse
import gzip
import io
import os
import xml.etree.ElementTree as ET

from pyspark import SparkContext
from pyspark.sql import SparkSession

from backend.etl.schema import article_schema

#############################################
def extract(gz_file):
    """doc string"""
    filename, file = gz_file
    print(f'Processing {filename}')

    file_obj = gzip.GzipFile(fileobj=io.BytesIO(file), mode='r')
    line = file_obj.readline()
    article_buffer = []
    recording = False
    while line:
        if b'<PubmedArticle>' in line:
            recording = True
        elif b'</PubmedArticle>' in line:
            article_buffer.append(line)
            recording = False
            current_recording = b''.join(article_buffer)
            article_buffer = []
            yield current_recording
        if recording:
            article_buffer.append(line)
        line = file_obj.readline()


def get_tag_text(root, tag_name):
    """doc string"""
    try:
        return root.find(tag_name).text
    except AttributeError:
        return None


def get_authors(article_node):
    """doc string"""
    authors = []
    is_first_author = True
    first_institution = None

    for author in article_node.iterfind('AuthorList/Author'):
        last_name = get_tag_text(author, 'LastName')
        fore_name = get_tag_text(author, 'ForeName')
        initials  = get_tag_text(author, 'Initials')
        affiliations = []
        for affiliation in author.iterfind('AffiliationInfo/Affiliation'):
            if is_first_author:
                first_institution = affiliation.text
                is_first_author = False
            affiliations.append(affiliation.text)

        authors.append((
            last_name,
            fore_name,
            initials,
            affiliations,
        ))

    invalid_fields = []

    if authors:
        for index, author in enumerate(authors):
            for tag, value in zip(['LastName, ForeName, Initials', 'Affiliations'], author):
                if not value:
                    invalid_fields.append(f'Author_{index}_{tag}')
    else:
        invalid_fields.append('Authors')

    return authors, first_institution, invalid_fields


def parse_article(pubmed_article):
    """doc string"""
    root = ET.fromstring(pubmed_article)
    # get the tag text under root element
    pmid  = get_tag_text(root, 'MedlineCitation/PMID')
    year  = get_tag_text(root, 'MedlineCitation/DateCompleted/Year')
    month = get_tag_text(root, 'MedlineCitation/DateCompleted/Month')
    day   = get_tag_text(root, 'MedlineCitation/DateCompleted/Day')
    # get the tag text under article element
    article = root.find('MedlineCitation/Article')
    article_title = get_tag_text(article, 'ArticleTitle')
    journal_title = get_tag_text(article, 'Journal/Title')
    # get info of authors and their affiliations
    authors, first_institution, invalid_fields = get_authors(article)

    # Append missing fields which is of interest to a list
    for tag, tag_name in zip(
        [pmid, year, month, day, article_title, journal_title, first_institution],
        'PMID Year Month Day ArticleTitle JournalTitle firstInst'.split(),
    ):
        if tag is None:
            invalid_fields.append(tag_name)

    if invalid_fields:
        validity = False
    else:
        invalid_fields = None  # rewrite to None to be recognized in df
        validity = True

    return pmid, year, month, day, article_title, \
           journal_title, authors, validity, invalid_fields, \
           first_institution


def parse(input_path, repartition_num, output_path, output_filename, spark_context):
    """doc string"""
    articles = spark_context.binaryFiles(input_path) \
        .flatMap(extract) \
        .repartition(int(repartition_num)) \
        .map(parse_article)
    spark = SparkSession(spark_context)
    articles_df = spark.createDataFrame(articles, article_schema)
    articles_df = articles.toDF(schema=article_schema)
    articles_df.write.format('parquet') \
            .mode('overwrite') \
            .save(os.path.join(output_path, output_filename))

def main(arguments):
    """docs string"""
    spark_context = SparkContext()
    parse(
        arguments.input_path,
        arguments.repartition_num,
        arguments.output_path,
        arguments.output_filename,
        spark_context
    )
    spark_context.stop()


if __name__ == '__main__':

    #############################################
    # args parser
    #############################################
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output_dir", dest="output_path",
                        default=".", help="Output directory where you store parquet file")
    parser.add_argument("-f", "--output_filename", dest="output_filename",
                        help="Output filename")  # delete default to make it as a mandatory argument
    parser.add_argument("-i", "--input_dir", dest="input_path",
                        default="../data/gz/", help="Input directory")
    parser.add_argument("-rep", "--repartition_number", dest="repartition_num",
                        default=4, help="Assign the number of partitions")

    args = parser.parse_args()
    if args.output_filename:
        main(args)
