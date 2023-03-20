"""
This is an example test script to go through the functions we defined in backend
OBS: it is given the assumption that the structure in backend_test is identical in the directory of backend
You can run pytest test_file_name.py to get the test result
"""

import pytest
import os
import sys

# To get the absolute path of functions to be tested by pytest
ORIGIN_DIR = os.path.dirname(os.path.abspath(__file__)).replace('backend_test', 'backend')
sys.path.insert(0, ORIGIN_DIR)

from equation import plus

"""
use pytest.approx() to wrap expected return value
"""


"""We use class based tests to work on individual functions"""
class TestPlus(object):
    def test_on_one_pls_one(self):
        actual = plus(1,1)
        # with pytest.raises(TypeError) as exception_info:
        #     plus(1, 'a')
        # assert exception_info.match(
        #     "Argument is supposed to have integeres as input"
        # )
        expected = 2
        message = ('the actual value {} is not equal to equation {} with the expected value'.format(
            actual,
            plus.__name__,
            expected
        ))
        assert actual is expected, message
    
    def test_on_non_number(self):
        test_arguments = 'a', 'b' 'c'
        # if function raises expected ValueError, test will pass
        # ifde function is buggy and does not raise ValueError, test will fail
        with pytest.raises(TypeError):
            plus(*test_arguments)    

    def test_on_extra_args(self):
        test_arguments = 1, 2, 3
        with pytest.raises(TypeError):
            plus(*test_arguments)

