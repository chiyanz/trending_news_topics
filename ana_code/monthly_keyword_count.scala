// load the cleaned df
val df = spark.read.option("header", "true").csv("classified_news_v2.csv")

// explode the keywords column
val explodedDF = processedDF.withColumn("keyword", explode(col("keywords")))
val keywordCounts = explodedDF.groupBy("year_month", "keyword")

val dfWithParsedKeywords = df.withColumn("keywords", split(regexp_replace($"keywords", "[\\[\\]]", ""), ",\\s*"))

val explodedDf = dfWithParsedKeywords.withColumn("keyword", explode($"keywords"))
val keywordCounts = explodedDf.groupBy("year_month", "keyword").count().orderBy("year_month", "count")

keywordCounts.show()

// store the resulting dataframe
keywordCounts.withColumn("keywords", col("keywords").cast("string"))
  .coalesce(1)
  .write
  .option("header", "true")
  .csv("monthly_topics")