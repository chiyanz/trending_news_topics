# trending_news_topics
Using big data tools to process, organize and extract trending topics from web scrapped news agencies. Built using Hadoop, Spark, and more.

## Porject Structure 
This goal of this project is to load data from existing news article databases, perform large scale data cleaning and organization such they can by used to achieve our goal: extract the trend of trending topics across different news publishers to see if they are similar.

The directories includes are:
/ana_code
/data_ingest
/output_data
/etl_code
/profiling_code
/screenshots

## Usage
The news organizations being analyzed so far are CNBC and Huffspot, the data is downloaded directly from data.world open source web scrapping datasets:
[CNBC](https://data.world/crawlfeeds/cnbc-news-dataset)
[Huffspot](https://data.world/crawlfeeds/huffspot-news-dataset)

The downloaded datasets can be found under the /data_ingest directory.

Analytic code will reference data stored in the data_ingest directory, no update to path is needed once the entire folder is uploaded.

There are multiple tasks that were done: data profiling, data cleaning, and data analysis. 
Data profiling is done in the /profiling_code folder
Data cleaning is done in the /etl_code folder
Data analytics is done in the /ana_code folder

Ther is no mutual dependency between the folders, and the bulk of our analysis can be found in the ana_code folder. 
Formatted output data extracted from HDFS can be found in output_data folder

Screenshots of code running and outputs can be found in /screenshots

The **final** resulting csv output is included in output and named classified_news_v2.csv

## Roadmap
- [x] Data Selection
- [x] Data Cleaning
- [x] Data Profiling 
- [x] Standarization of datasets and keyword extraction
- [x] News category classification
- [ ] SQL implementation through Hive/Presto (WIP)
