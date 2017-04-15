#!/usr/bin/env bash
aws s3 cp s3://warren-datasets/spark-corenlp-0.2.0-s_2.10.jar ./spark-corenlp-0.2.0-s_2.10.jar
aws s3 cp s3://warren-datasets/stanford-english-kbp-corenlp-2016-10-31-models.jar ./stanford-english-kbp-corenlp-2016-10-31-models.jar
aws s3 cp s3://warren-datasets/stanford-english-corenlp-2016-10-31-models.jar ./stanford-english-corenlp-2016-10-31-models.jar
aws s3 cp s3://warren-datasets/stanford-corenlp-full-2016-10-31.zip ./stanford-corenlp-full-2016-10-31.zip
tar -xvf stanford-corenlp-full-2016-10-31.zip

sudo mv ./spark-corenlp-0.2.0-s_2.10.jar /usr/lib/spark/jars/spark-corenlp-0.2.0-s_2.10.jar
sudo mv ./stanford-english-kbp-corenlp-2016-10-31-models.jar /usr/lib/spark/jars/stanford-english-kbp-corenlp-2016-10-31-models.jar
sudo mv ./stanford-english-corenlp-2016-10-31-models.jar /usr/lib/spark/jars/stanford-english-corenlp-2016-10-31-models.jar