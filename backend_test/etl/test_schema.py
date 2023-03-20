"""module to test customized schema: author_type, article_schema"""

import pytest

from backend.etl.schema import author_type, article_schema

def field_test_helper(json, field_name, data_type, nullable):
    """a help function to check name, type and nullability given a field"""
    actual_name     = json.get('name')
    actual_type     = json.get('type')
    actual_nullable = json.get('nullable')
    assert actual_name == field_name, \
            f'The field {field_name} got a different name, {actual_name}'
    assert actual_type == data_type, \
            f'The field {field_name} is expected to have {data_type}, but got {actual_type}'
    assert actual_nullable == nullable, \
            f'The nullability of field {field_name} is expected to be {nullable}, \
              but got {actual_nullable}'

class TestAuthorType:
    """test class on author type"""
    fields = author_type.fields

    def test_author_type_existance(self):
        """test the existance of author_type"""
        assert self.fields, 'Author_type is empty'

    def test_author_type_on_last_name(self):
        """test the field of last name"""
        last_name_json = self.fields[0].jsonValue()
        field_test_helper(last_name_json, 'lastName', 'string', True)

    def test_author_type_on_fore_name(self):
        """test the field of fore name"""
        fore_name_json = self.fields[1].jsonValue()
        field_test_helper(fore_name_json, 'foreName', 'string', True)

    @pytest.mark.skip(reason="test case not implemented")
    def test_author_type_on_initials(self):
        """test the field of initials"""
        assert True

    def test_author_type_on_affiliations(self):
        """test the field of affiliations"""
        affiliations_json = self.fields[3].jsonValue()
        field_test_helper(
            affiliations_json,
            'affiliations',
            {'type': 'array', 'elementType': 'string', 'containsNull': True},
            True
        )

class TestArticleSchema:
    """test article_schema"""
    fields = article_schema.fields

    def test_article_schema_existance(self):
        """test the existance of article_schema"""
        assert self.fields, 'Article schema not exists'

    def test_article_schema_on_pmid(self):
        """test the field of pubmed identity"""
        pmid_json = self.fields[0].jsonValue()
        field_test_helper(pmid_json, 'pmid', 'string', True)

    def test_article_schema_on_year(self):
        """test the field of completed year"""
        year_json = self.fields[1].jsonValue()
        field_test_helper(year_json, 'year', 'string', True)

    def test_article_schema_on_month(self):
        """test the field of completed month"""
        month_json = self.fields[2].jsonValue()
        field_test_helper(month_json, 'month', 'string', True)

    def test_article_schema_on_day(self):
        """test the field of completed day"""
        day_json = self.fields[3].jsonValue()
        field_test_helper(day_json, 'day', 'string', True)

    def test_article_schema_on_title(self):
        """test the field of article title"""
        title_json = self.fields[4].jsonValue()
        field_test_helper(title_json, 'title', 'string', True)

    def test_article_schema_on_journal_title(self):
        """test the field of journal title"""
        journal_json = self.fields[5].jsonValue()
        field_test_helper(journal_json, 'journalTitle', 'string', True)

    def test_article_schema_on_author_list(self):
        """test the field of author list"""
        authors_json = self.fields[6].jsonValue()
        field_test_helper(
            authors_json,
            'authorList',
            {
                'type': 'array',
                'elementType': {
                    'type': 'struct',
                    'fields': [
                        {
                            'name': 'lastName',
                            'type': 'string',
                            'nullable': True,
                            'metadata': {}
                        },
                        {
                            'name': 'foreName',
                            'type': 'string',
                            'nullable': True,
                            'metadata': {}
                        },
                        {
                            'name': 'initials',
                            'type': 'string',
                            'nullable': True,
                            'metadata': {}
                        },
                        {
                            'name': 'affiliations',
                            'type': {
                                'type': 'array',
                                'elementType': 'string',
                                'containsNull': True
                            },
                            'nullable': True,
                            'metadata': {}
                        }
                    ]
                },
                'containsNull': True
            },
            True
        )

    def test_article_schema_on_validity(self):
        """test the field of validity"""
        validity_json = self.fields[7].jsonValue()
        field_test_helper(validity_json, 'validity', 'boolean', False)

    def test_article_schema_on_invalid_fields(self):
        """test the field of invalid fields"""
        invalid_fields_json = self.fields[8].jsonValue()
        field_test_helper(
            invalid_fields_json,
            'invalidFields',
            {
                'type': 'array',
                'elementType': 'string',
                'containsNull': True
            },
            True
        )

    def test_article_schema_on_first_inst(self):
        """test the field of first institution"""
        first_inst_json = self.fields[9].jsonValue()
        field_test_helper(first_inst_json, 'firstInst', 'string', True)
