create external table news_trend(published_at STRING, keywords STRING, year_month STRING, source STRING, entertainment INT, health INT, politics INT, finance INT, technology INT) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\"",
    "escapeChar"    = "\\"
) STORED AS TEXTFILE location 'hdfs://nyu-dataproc-m/user/qz2190_nyu_edu/classified_news' TBLPROPERTIES ("skip.header.line.count"="1");


