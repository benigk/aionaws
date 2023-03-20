sudo yum update -y
sudo yum -y install docker
sudo service docker start

pip3 install pycountry
pip3 install mordecai
pip3 install fsspec

python3 -m spacy download en_core_web_lg

docker pull elasticsearch:5.5.2
wget https://andrewhalterman.com/files/geonames_index.tar.gz --output-file=wget_log.txt
tar -xzf geonames_index.tar.gz
docker run -d -p 127.0.0.1:9200:9200 -v $(pwd)/geonames_index/:/usr/share/elasticsearch/data elasticsearch:5.5.2