val df = spark.read.option("header", "true").csv("news_classified.csv")

val explodedDF = processedDF.withColumn("keyword", explode(col("keywords")))
val keywordCounts = explodedDF.groupBy("year_month", "keyword")
