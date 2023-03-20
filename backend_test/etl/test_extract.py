"""
module to test functions from extract.py
The functions to be tested are:
    extract
    parse_article
"""
from pathlib import Path
import os

import pytest

# import the module containing the functions to be tested
from backend.etl import extract


TEST_BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = os.path.join(str(TEST_BASE_DIR), 'data')

class TestExtract:
    """test class for 'def extract()'"""
    def test_on_extract_with_string_as_input(self):
        """it tests if the file contains bytes-like object"""
        with pytest.raises(TypeError):
            list(extract.extract(('filename', 'string')))

    def test_on_extract_using_article_tag(self):
        """doc string"""
        expected_article_number = 5
        input_test_file_path = os.path.join(DATA_DIR, 'gz', 'pubmed_10articles.xml.gz')
        with open(input_test_file_path, 'rb') as test_f:
            input_bytes = b''.join(test_f.readlines())
        actual_article_number = len(list(extract.extract(('filename', input_bytes))))
        assert expected_article_number == actual_article_number, \
            f'There are {expected_article_number} articles included \
            in the test file pubmed_10articles.xml.gz, \
            but got {actual_article_number} instead'

# class TestGetTagText:
#     """doc string"""
#     pass

# class TestGetAuthors:
#     """doc string"""
#     pass

class TestParseArticle:
    """doc string"""
    def get_article_string(self, filename, dirname='xml'):
        """doc string"""
        with open(os.path.join(DATA_DIR, dirname, filename), mode='r', encoding='utf-8') as test_f:
            return ''.join([line for line in test_f.readlines() if 'PubmedArticleSet' not in line ])

    def test_on_parse_article_with_missing_pmid(self):
        """doc string"""
        article_string = self.get_article_string('article_with_missing_pmid.xml')
        extracted_features = extract.parse_article(article_string)
        assert extracted_features[0] is None, \
            'The missing PMID tag is not correctly set to None'
        assert extracted_features[7] is False, \
            f'The Validity is expected to be False, \
            but got {extracted_features[7]} instead'

    def test_on_parse_article_with_missing_dates(self):
        """doc string"""
        article_string = self.get_article_string('article_with_missing_dates.xml')
        extracted_features = extract.parse_article(article_string)
        for feature, tag in zip(extracted_features[1:4], 'Year Month Day'.split()):
            assert feature is None, \
                f'The missing {tag} tag is not correct set to None'
            assert tag in extracted_features[8], \
                f'The tag {tag} is not included in the invalid fields'
        assert extracted_features[7] is False, \
                f'The Validity is expected to be False, \
                but got {extracted_features[7]} instead'

    def test_on_parse_article_with_missing_authors(self):
        """doc string"""
        article_string = self.get_article_string('article_with_missing_authors.xml')
        extracted_features = extract.parse_article(article_string)
        assert isinstance(extracted_features[6], list) and not extracted_features[6], \
                f'The authors are expected to be empty, \
                but got {extracted_features[6]} instead'
        assert extracted_features[7] is False, \
                f'The Validity tag is expected to be False, \
                but got {extracted_features[7]} instead'
        assert 'Authors' in extracted_features[8], \
                'The invalid fields do not include "Authors"'

    def test_parse_article_on_pmid_extraction(self):
        """doc string"""
        article_string = self.get_article_string('article.xml')
        extracted_features = extract.parse_article(article_string)
        expected_pmid = "30804456"
        assert extracted_features[0] == expected_pmid, \
            f'The expected PMID is {expected_pmid}, but got {extracted_features[0]} instead'


    def test_parse_article_on_year_extraction(self):
        """doc string"""
        article_string = self.get_article_string('article.xml')
        extracted_features = extract.parse_article(article_string)
        expected_year = "2019"
        assert extracted_features[1] == expected_year, \
            f'The expected Year is {expected_year}, but got {extracted_features[1]} instead'

    def test_parse_article_on_month_extraction(self):
        """doc string"""
        article_string = self.get_article_string('article.xml')
        extracted_features = extract.parse_article(article_string)
        expected_month = "12"
        assert extracted_features[2] == expected_month, \
            f'The expected month is {expected_month}, but got {extracted_features[2]} instead'

    def test_parse_article_on_title_extraction(self):
        """doc string"""
        article_string = self.get_article_string('article.xml')
        extracted_features = extract.parse_article(article_string)
        expected_article_title = ('The inflammatory cytokine IL-6 induces FRA1 deacetylation'
            ' promoting colorectal cancer stem-like properties.')

        assert extracted_features[4] == expected_article_title, \
            f'The expected article title is {expected_article_title}, \
            but got {extracted_features[4]} instead'

    def test_parse_article_on_journal_title_extraction(self):
        """doc string"""
        article_string = self.get_article_string('article.xml')
        extracted_features = extract.parse_article(article_string)
        expected_journal_title = 'Oncogene'
        assert extracted_features[5] == expected_journal_title, \
            f'The expected journal titile is {expected_journal_title},\
             but got {extracted_features[5]} instead'

    def test_parse_article_on_author_list_extraction(self):
        """doc string"""
        article_string = self.get_article_string('article.xml')
        extracted_features = extract.parse_article(article_string)
        expected_authors = [
            ('Wang', 'Tingyang', 'T',
                [
                    'Department of Pathology & Pathophysiology,'
                    ' and Cancer Institute of the Second Affiliated Hospital,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.',
                    'Key Laboratory of Disease Proteomics of Zhejiang Province,'
                    ' Key Laboratory of Cancer Prevention and '
                    'Intervention of China National Ministry of Education,'
                    ' and Research Center for Air Pollution and Health,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.'
                ]
            ),
            ('Song', 'Ping', 'P',
                [
                    'Department of Pathology & Pathophysiology,'
                    ' and Cancer Institute of the Second Affiliated Hospital,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.',
                    'Key Laboratory of Disease Proteomics of Zhejiang Province,'
                    ' Key Laboratory of Cancer Prevention and'
                    ' Intervention of China National Ministry of Education,'
                    ' and Research Center for Air Pollution and Health,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.'
                ]
            ),
            ('Zhong', 'Tingting', 'T',
                [
                    'Department of Pathology & Pathophysiology,'
                    ' and Cancer Institute of the Second Affiliated Hospital,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.',
                    'Department of Pathology, Sir Run Run Shaw Hospital,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.'
                ]
            ),
            ('Wang', 'Xianjun', 'X',
                [
                    'Department of Pathology & Pathophysiology,'
                    ' and Cancer Institute of the Second Affiliated Hospital,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.'
                ]
            ),
            ('Xiang', 'Xueping', 'X',
                [
                    'Department of Pathology & Pathophysiology,'
                    ' and Cancer Institute of the Second Affiliated Hospital,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.',
                    'Key Laboratory of Disease Proteomics of Zhejiang Province,'
                    ' Key Laboratory of Cancer Prevention and'
                    ' Intervention of China National Ministry of Education,'
                    ' and Research Center for Air Pollution and Health,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.'
                ]
            ),
            ('Liu', 'Qian', 'Q',
                [
                    'Department of Pathology & Pathophysiology,'
                    ' and Cancer Institute of the Second Affiliated Hospital,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.'
                ]
            ),
            ('Chen', 'Haiyi', 'H',
                [
                    'College of Pharmaceutical Sciences,'
                    ' Zhejiang University, Hangzhou, China.'
                ]
            ),
            ('Xia', 'Tian', 'T',
                [
                    'Department of Pathology & Pathophysiology,'
                    ' and Cancer Institute of the Second Affiliated Hospital,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.'
                ]
            ),
            ('Liu', 'Hong', 'H',
                [
                    'Department of Pathology & Pathophysiology,'
                    ' and Cancer Institute of the Second Affiliated Hospital,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.',
                    "Zhejiang Normal University-Jinhua People's Hospital Joint Center"
                    " for Biomedical Research, Jinhua, China."
                ]
            ),
            ('Niu', 'Yumiao', 'Y',
                [
                    'Department of Pathology & Pathophysiology,'
                    ' and Cancer Institute of the Second Affiliated Hospital,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.'
                ]
            ),
            ('Hu', 'Yanshi', 'Y',
                [
                    'Department of Bioinformatics, College of Life Sciences,'
                    ' Zhejiang University, Hangzhou, China.'
                ]
            ),
            ('Xu', 'Lei', 'L',
                ['Institute of Bioinformatics and Medical Engineering,'
                ' School of Electrical and Information Engineering,'
                ' Jiangsu University of Technology, Changzhou, China.'
                ]
            ),
            ('Shao', 'Yingkuan', 'Y',
                [
                    'Department of Pathology & Pathophysiology,'
                    ' and Cancer Institute of the Second Affiliated Hospital,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.',
                    'Key Laboratory of Disease Proteomics of Zhejiang Province,'
                    ' Key Laboratory of Cancer Prevention and'
                    ' Intervention of China National Ministry of Education,'
                    ' and Research Center for Air Pollution and Health,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.'
                ]
            ),
            ('Zhu', 'Lijun', 'L',
                [
                    'Key Laboratory of Precision Diagnosis and'
                    ' Treatment for Hepatobiliary and'
                    ' Pancreatic Tumor of Zhejiang Province,'
                    ' First Affiliated Hospital, Zhejiang University School of Medicine,'
                    ' Hangzhou, China.'
                ]
            ),
            ('Qi', 'Hongyan', 'H',
                [
                    'Department of Pathology & Pathophysiology,'
                    ' and Cancer Institute of the Second Affiliated Hospital,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.'
                ]
            ),
            ('Shen', 'Jing', 'J',
                [
                    'Department of Pathology & Pathophysiology,'
                    ' and Cancer Institute of the Second Affiliated Hospital,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.'
                ]
            ),
            ('Hou', 'Tingjun', 'T',
                [
                    'College of Pharmaceutical Sciences,'
                    ' Zhejiang University, Hangzhou, China.',
                    'Institute of Bioinformatics and Medical Engineering,'
                    ' School of Electrical and Information Engineering,'
                    ' Jiangsu University of Technology, Changzhou, China.'
                ]
            ),
            ('Fodde', 'Riccardo', 'R',
                [
                    'Department of Pathology, Erasmus MC Cancer Institute,'
                    ' Erasmus University Medical Center, Rotterdam, The Netherlands.'
                    ' r.fodde@erasmusmc.nl.'
                ]
            ),
            ('Shao', 'Jimin', 'J',
                [
                    'Department of Pathology & Pathophysiology,'
                    ' and Cancer Institute of the Second Affiliated Hospital,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.'
                    ' shaojimin@zju.edu.cn.',
                    'Key Laboratory of Disease Proteomics of Zhejiang Province,'
                    ' Key Laboratory of Cancer Prevention and'
                    ' Intervention of China National Ministry of Education,'
                    ' and Research Center for Air Pollution and Health,'
                    ' Zhejiang University School of Medicine, Hangzhou, China.'
                    ' shaojimin@zju.edu.cn.'
                ]
            )
        ]
        assert extracted_features[6] == expected_authors, \
            f'The expected author is {expected_authors}, but got {extracted_features[6]} instead'


    def test_parse_article_on_validity_verification(self):
        """doc string"""
        article_string = self.get_article_string('article.xml')
        extracted_features = extract.parse_article(article_string)
        expected_validity = True
        assert extracted_features[7] == expected_validity, \
            f'The expected validity is {expected_validity}, but got {extracted_features[7]} instead'


    @pytest.mark.skip('test method not completed')
    def test_on_parse_article_on_invalid_fields_verification(self):
        """doc string"""

    def test_parse_article_on_first_inst_extraction(self):
        """doc string"""
        article_string = self.get_article_string('article.xml')
        extracted_features = extract.parse_article(article_string)
        expected_first_institution = ('Department of Pathology & Pathophysiology, '
                                     'and Cancer Institute of the Second Affiliated Hospital, '
                                     'Zhejiang University School of Medicine, Hangzhou, China.')
        assert extracted_features[9] == expected_first_institution, \
        f'The expected first institution is {expected_first_institution}, \
        but got {extracted_features[9]} instead'

# The following test case is to be written into integration test
# Motivation: we provide a fast loop feedback given frequently used test cases.
# The integration test is done less frequently and combine different parts of the project.
# class TestParse(object):
#     def test_on_parse(self):
#         kwargs = {
#             'input_path': os.path.join(DATA_DIR, 'gz'),
#             'repartition_num': 4,
#             'output_path': '.',
#             'output_filename': '60k.parquet',
#             'sc': sc,
#             'spark': spark,
#         }
#         extract.parse(**kwargs)
#         # check if file has been created
#         assert os.path.isdir(os.path.join(CUR_DIR, '60k.parquet')), "The file does not exist"
#         df = spark.read.parquet(os.path.join(CUR_DIR, '60k.parquet'))
#         actual_count = df.count()
#         expected_count = 60000
#         assert actual_count==expected_count, \
#           f'The DataFrame is expected to get {expected_count} rows, \
#           but got {actual_count} instead'
#         shutil.rmtree(os.path.join(CUR_DIR, '60k.parquet'))
