CREATE TABLE trozamon_testing_ngrams
(ngram STRING, year INT, count BIGINT, volumes BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

LOAD DATA INPATH 'trozamon_testing/input_structured'
OVERWRITE INTO TABLE trozamon_testing_ngrams;

SELECT COUNT(*) FROM trozamon_testing_ngrams;

DROP TABLE trozamon_testing_ngrams;
