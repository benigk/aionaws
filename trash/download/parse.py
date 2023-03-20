#! /home/rex/thesis/bin/python

"""
create classes to parse and store the data for further analysis
"""

from typing import DefaultDict

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


class Affiliation:
    affiliation_dict = DefaultDict(int)
    
    def __init__(self, affiliation):
        self.affiliation = affiliation.text
        self.affiliation_dict[self.affiliation] += 1

    def __str__(self):
        return self.affiliation


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
                self.foreName = child.text
            elif lowered_tag == 'initials':
                self.initials = child.text
            elif lowered_tag == 'affiliationinfo':
                self.affilitionList = [Affiliation(c) for c in list(child)]
            else:
                self.unprocessed_tags[child.tag] += 1
        self.author_list.append(self.__str__)
    def __str__(self):
        return '%s %s' % (self.lastName, self.forename)


class PublicationType:
    publication_type_dict = DefaultDict(list)
    
    def __init__(self, publicationTypeElement):
        self.publicationType = publicationTypeElement.text
    
    def __str__(self):
        return self.publicationType


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

class PubmedArticle:
    unprocessed_tags = DefaultDict(int)
    def __init__(self, article):
        self.PMID, self.medlinecitation, self.pubmedData = None, None, None
        self._feats = {} # here stores the features to be stored in .parquet
        for child in list(article):
            lowered_tag = child.tag.lower()
            if lowered_tag == 'medlinecitation':
                self.medlinecitation = MedlineCitation(child)
                self.PMID = self.medlinecitation.PMID
            elif lowered_tag == 'pubmeddata':
                self.pubmedData = PubmedData(child)
            else:
                self.unprocessed_tags[child.tag] += 1

        self.feats = {
            'pmid': self.medlinecitation.PMID,
            'title': self.medlinecitation.article.articleTitle,
            'journal_title': self.medlinecitation.article.journal.title,
            'author_list': [
                (
                    author.foreName, 
                    author.lastName,
                    author.initials,
                    *[str(affiliation) for affiliation in author.affilitionList]
                ) for author in self.medlinecitation.article.authorList 
                ],
        }

        def get_features(self, columns):
            try:
                return tuple(self.feats[column] for column in columns)
            except KeyError:
                unknown_features = [c for c in columns if c not in self.feats]
                print('The following features are not detected. Check if it exists in pubmed data and you have added in the parsing.')
                print(' '.join(unknown_features))
            except Exception:
                print('Error in parsing')
        