# What do we need to consider?

* The environment to test(e.g., memory threshold)
* How many test cases do we need
* What kind of test cases do we need
* Use pytest.ini to set up test environment
# Why do we need xml file since we have already gz source files?

* To create independent test cases
* Save time and make test more efficiently
* Dedicated built to test the funtion, parse_article()

```
# snippet code to generate xml files from gz and pyspark

files = sc.binaryFiles(GZ_DIR)
with open('articles.xml', code='w', encoding='utf-8') as f:
    for file in files.collect():
        for article in list(parse_xml.extract(file))[:10]:
            f.write(article.decode('utf-8'))
            f.write('\n')
```

# A good resource using pytest-case to organize test cases in a better way
[pytest-cases](https://smarie.github.io/python-pytest-cases/)