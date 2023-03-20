"""
Functions to be tested:

1. extract_location(input: string)
    output: tuple(String:Country, String:City, String:Address)
2. extract_coordinates(input: string)
    output: tuple(String:Longitude, String:Latitude)
"""

import pytest

from backend.etl import transform

class TestExtractLocation(object):

    @pytest.mark.skip(reason="the test case is not fulfilled")
    def test_on_extract_location_with_same_place_but_different_string_input(self):
        place_name_1 = None
        place_name_2 = None

        assert transform.extract_location(
            place_name_1) == transform.extract_location(place_name_2)

    @pytest.mark.skip(reason="the test case is not fulfilled")
    def test_on_extract_location_with_common_input(self):

        input_string = ''  # we need to find some common location string to test with
        #
        #
        pass

    def test_on_extract_location_with_total_parsing_failure(self):
        expected_parsed_location = 'An unparsable location string', None, None
        unparsable_location_string = 'An unparsable location string'
        actual_parsed_location = transform.extract_location(
            unparsable_location_string)
        assert expected_parsed_location == actual_parsed_location, \
            'Unparsable_location_string is expected to get {}, but got {} instead'.format(
                expected_parsed_location, actual_parsed_location
            )

    def test_on_extract_location_with_correct_output_format(self):
        actual = transform.extract_location(
            'Department of Neurosurgery, Beijing Hospital, National Center of Gerontology, Dong Dan, Beijing, China.')
        assert isinstance(actual, tuple), \
            'It is expected to have tuple as output, but got {} instead'.format(
                type(actual))
        assert len(actual) == 3, \
            'it is expected to have the length of 3 as output, but got {} instead'.format(
                len(actual))

        for index, element in enumerate(actual):
            assert isinstance(element, str), \
                'It is expected to have a string in the element {} in output tuple, but got {} instead'.format(
                    index, type(element)
            )


class TestExtractCoordinates(object):

    def test_on_extract_coordinates_with_unknown_location(self):
        expected_parsed_coordinates = None, None, None
        unknown_place = 'UNKOWN_PLACE'
        actual_parsed_coordinates = transform.extract_coordinates(
            unknown_place)
        assert expected_parsed_coordinates == actual_parsed_coordinates, \
            'The expected parsed location given the string {} is {}, but got {} instead'.format(
                unknown_place, expected_parsed_coordinates, actual_parsed_coordinates
            )

    def test_on_extract_coordinates_with_country(self):
        """
        problem description:
        how do we define success if the coordinate to show for the country level is appropriate
        is it good enough that the parsed coordinates is located in the country area, 
        do we have any deivation tolerance when we assert if it is accurate enough
        """
        expected_parsed_coordinates = ('-89.35478', '29.2605', 'USA')
        country = 'USA'
        actual_parsed_coordinates = transform.extract_coordinates(country)
        assert expected_parsed_coordinates == actual_parsed_coordinates, \
            'The expected parsed coordinates given the country of {}, is {}, but got {} instead'.format(
                expected_parsed_coordinates, country, actual_parsed_coordinates
            )

    @pytest.mark.skip(reason="the test case is not fulfilled")
    def test_on_extract_coordinates_with_city(self):
        """
        We ask for the precision threshold for a given coordinates
        """
        expected_parsed_coodinates = ()
        city = 'London'
        actual_parsed_coordinates = transform.extract_coordinates(city)
        assert expected_parsed_coodinates == actual_parsed_coordinates, \
            'The expected parsed coordiantes given the city of {}, is {}, but got {} instead'.format(
                expected_parsed_coodinates, city, actual_parsed_coordinates
            )

    @pytest.mark.skip(reason="the test case is not fulfilled")
    def test_on_extract_coordinates_with_affiliation(self):
        # check with affiliation of 'Chalmers University of Technology'
        assert False

    def test_on_extract_coordinates_with_non_string(self):
        with pytest.raises(TypeError):
            transform.extract_coordinates(None)

    @pytest.mark.skip(reason="the test case is not fulfilled")
    def test_on_extract_coordinates_with_correct_output_format(self):
        """check the equivalent part for the function of extract_location"""
        assert False
