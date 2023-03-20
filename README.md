# Thesis Work 2022 at Software by Quokka 

# How to run?

# Project Structure

# Thesis Work at Software by Quokka

## Data

[PubMed](https://pubmed.ncbi.nlm.nih.gov) comprises more than 33 million citations for biomedical literature from MEDLINE, life science journals, and onlin books. Check the original [API](https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/) of PubMed to download the source files. Be aware that the latest added file often comes in the last pages. 

## Storage

We have 20 files, which occur in the last pages from the given API, stored in our **AWS s3** (s3a://quokka-2021-thesis/pubmed21-part/). Each file contains 30 000 article metadata and is around 50 MB. It is possible to download these files on local device as a starting point to investigate. After you have configured AWS setting, you can download the files from AWS bucket using `download.sh` by `bash download.sh OUTPUT_DIR`.  

## Extraction

For now, we focus on the features of authors and their affiliations in the dataset. Therefore, we extract each article with a unique Pubmed ID, article title, journal title and author list with a list of affiliations for each author. 

## Pyspark

Spark enables us a greate power to handle big data. We expect to use pyspark to deal with the data. In this repository, we provide a basic version to parse the xml files using pyspark and store them in the format of [parquet](https://databricks.com/glossary/what-is-parquet). The file named `600k.parquet` is the result after parsing these 20 files with 600 000 articles.

## Pytest

### Configuration
https://docs.pytest.org/en/6.2.x/customize.html

# Pilot Study

In `pilot_study.ipynb`, we illustrate a first glimpse how we can pyspark to transform our data given the following requests:


1. Cut the author list from each article and expand the author list into differet rows where each author stand on own row with their affiliations
2. Expand the affiliation list on different rows
3. Parse the affiliaton and detect the possible location names
4. We use PMID to keep track the article throughout authors and affiliations

## The problems shown from the pilot study:
* Missing data (Some names are missing, affiliations can be null)
* PMID can be duplicate (Only two cases)
* Location detector performs badly. A lot of location tokens are not successfully detected. We need a much better one.

## What's next?

* Data cleaning: to filter invalid data (article that does not meet the requirements or does not contain full features we are interested in). 
* To generate a parquet file as raw input files
* To include time, e.g. the time when the article is released, into our data. This is because time can be an important dimension to cluster articles.
* To narrow down our scope of which files we are looking into, we may focus on the articles in Sweden, given the motivation that Sweden is not a big country and it is easier to get feedback from local clients/organizations. One potential use case is to provide data support for the students to select university when comparing the strength of biological science.
* Location parser. The parser from nltk library is not good to detect location in an affiliation string, especailly when location is not well-known in the English world. 


# Pubmed Data Structure

Instruction: here illustrates the data structure through an example article. It is worth to be aware that not all articles follow the same structure as is shown below. In some extreme cases, the text value for a common tag like `ForeName` can be missing or `PMID` can be duplicate. The tag shown in original xml file does not contain any space. The inserted space in our snippet is to help you quick understand the field it refers to.

Use the following data structure to 
* help you to parse xml files 
* find the interesting features in our pubmed metadata


# Data Structure Example (Article PMID: 30773852)
* Pubmed Article Set
    * Pubmed Article
        * MedlineCitation
            * `PMID` (in use, as the unique id to keep track of article) 
            * `DateCompleted` (extracted)
            * Date Revised
            * Article
                * Journal
                    * ISSN
                    * JournalIssue
                        * Volume
                        * Issue
                        * PubDate
                    * `Title` (extracted)
                    * ISOAbbreviation
                * Article Title
                * Pagination
                    * MedlinePgn
                * ELocationID
                * Abstract
                    * Abstract Text
                    * CopyrightInformation
                * `Author List` (extracted)
                    * Author
                        * Last Name
                        * Fore Name
                        * Initials
                        * AffiliationInfo
                            * Affiliation
                * Language
                * Publication Type List
                    * Publication Type
                * Article Date
            * Medline Journal Info
                * Country
                * MedlineTA
                * Nlm Unique ID
                ISSN Linking
            * Citation Subset
            * Mesh Heading List
                * Mesh Heading
                    * Descriptir Name
                    * Qualifier Name
            * Keyword List
                * Keyword
        * Pubmed Data
            * History
                * PubMed Pub Date
            * Publication Status
            * Article Id List
                * Article Id

# GeoJson template
```
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
```

# Test CI
