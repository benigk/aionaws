
1. Mordecai is on PyPI and can be installed for Python 3 with pip:

```bash
pip install mordecai
```

Note: It's strongly recommended that you run Mordecai in a virtual environment. The libraries that Mordecai depends on are not always the most recent versions and using a virtual environment prevents libraries from being downgraded or running into other issues

2. You should then download the required spaCy NLP model:

```bash
python -m spacy download en_core_web_lg
```

3. In order to work, Mordecai needs access to a Geonames gazetteer running in Elasticsearch. The easiest way to set it up is by running the following commands (you must have Docker installed first).

```bash
docker pull elasticsearch:5.5.2
wget https://andrewhalterman.com/files/geonames_index.tar.gz --output-file=wget_log.txt
tar -xzf geonames_index.tar.gz
docker run -d -p 127.0.0.1:9200:9200 -v $(pwd)/geonames_index/:/usr/share/elasticsearch/data elasticsearch:5.5.2
```