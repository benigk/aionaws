# Foreword

This readme gives an introduction to use pytest and the ideas behind unit testting. e.g., why we need unit test. 
 
# Python unit testing libraries:

Four Python unit testing libraries are available:
* pytest (in use in our case)
* unittest
* nosetests
* doctest

</br>

* Motivation of using pytest:
    1. Includes all essential features;
    2. Easist to use
    3. Most popular


# Tips when we use unit test
1. how many and what types of test cases do we need for a function to be tested?
    1. bad arguments     (one or several)
    2. special arguments (case dependent)
    3. boundary values   (one or several)
    4. normal arguments  (multiple)
    * A function may not contain all types of arguments

2. project structure
    Here gives an example how we can organize the files when using unit test

    ```
    src/                                # All application code lives here
    |-- data/                           # Package for data preprocessing
    |   |-- __init__.py
    |   |--  preprocessing_helpers.py   # Contains functions to be tested
    |-- features/
    |   |-- __init__.py
    |   |-- as_numpy.py
    |-- models/
    |   |-- __init__.py
    |   |-- train.py
    
    # The tests folder mirrors the application folder
    
    tests/
    |-- data/
    |   |-- __init__.py
    |   |-- test_preprocessing_helpers.py
    |-- features/
    |   |-- __init__.py
    |   |-- test_as_numpy.py
    |-- models/
    |   |-- __init__.py
    |   |-- test_train.py
    ```

3. Test function or Test Class

Test class is a container for a single unit's test

