-- create 
CREATE TABLE IF NOT EXISTS news_data (
    publish_date TIMESTAMP,
    content STRING,
    col1 INT,
    col2 INT,
    col3 INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/news_data';

-- load 
LOAD DATA INPATH 'hdfs://nyu-dataproc-m/user/yc5508_nyu_edu/project/classified_news_v2.csv' INTO TABLE news_data;
