// note: because raw textual and html data is included, parsing is very difficult. 
// dataframe is used to first parse the input csv, analysis is done using RDD structure
var CNBCDataRDD1 = spark.read.csv("project/cnbc_news_datase.csv").rdd
val Head = CNBCDataRDD1.first()
val NoHeader = CNBCDataRDD1.filter(line => !line.equals(Head))

// task 1: create a key-value mapping of the RDD
val title_mappings = NoHeader.map(x => (x(0), x))
println("Number of news entries =" + title_mappings.count())

// task 2: show the number of unique values in each column
// first: map each column by iterating over the rows
val title = NoHeader.map(x => x(0))
val url = NoHeader.map(x => x(1))
val published_at = NoHeader.map(x => x(2))
val author = NoHeader.map(x => x(3))
val publisher = NoHeader.map(x => x(4))
val description = NoHeader.map(x => x(5))
val keywords = NoHeader.map(x => x(6))


// print the unique values in each column
val uniqueTitlesCount = title.distinct().count()
println(s"Number of unique titles: $uniqueTitlesCount")
val uniqueUrlsCount = url.distinct().count()
println(s"Number of unique URLs: $uniqueUrlsCount")
val uniquePublishedAtCount = published_at.distinct().count()
println(s"Number of unique publication dates: $uniquePublishedAtCount")
val uniqueAuthorsCount = author.distinct().count()
println(s"Number of unique authors: $uniqueAuthorsCount")
val uniquePublishersCount = publisher.distinct().count()
println(s"Number of unique publishers: $uniquePublishersCount")
val uniqueDescriptionsCount = description.distinct().count()
println(s"Number of unique descriptions: $uniqueDescriptionsCount")
val uniqueKeywordsCount = keywords.distinct().count()
println(s"Number of unique keywords sets: $uniqueKeywordsCount")


