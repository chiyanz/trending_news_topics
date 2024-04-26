var df = spark.read.option("header", "true").csv("project/cnbc_news_datase.csv")
val df2 = spark.read.option("header", "true").csv("project/huffpost_news_data.csv")

// parsing the date column
val df_parsedDate = df.withColumn("parsedDate", to_timestamp($"published_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
val df2_parsedDate = df2.withColumn("published_at", to_date($"published_at", "yyyy-MM-dd"))

// spliting keywords column of each csv into array
val df_parsedKeywords = df_parsedDate.withColumn("keywords", split($"keywords", ", "))
val df2_parsedKeywords = df2_parsedDate.withColumn("keywords", split($"tags", ","))

// text transform: trim and turn to lower case
val df_transformedKeywords = df_parsedKeywords.withColumn("keywords", transform($"keywords", f => lower(trim(f))))
val df2_transformedKeywords = df2_parsedKeywords.withColumn("keywords", transform($"keywords", f => lower(trim(f))))

// keep only relevant columns
val df1Adjusted = df_transformedKeywords.select("published_at", "keywords")
val df2Adjusted = df2_transformedKeywords.select("published_at", "keywords")

// combine 
val df_combined = df1Adjusted.union(df2Adjusted).select("published_at", "keywords")

// drop all columns with exceptions
val filtered_df = df_combined.na.drop(how="all")
val cleaned_df = filtered_df.withColumn("keywords", transform(col("keywords"), x => lower(x)))

// create a column that indicates if a news article is an entertainment article
val final_df = cleaned_df.withColumn("entertainment", when(array_contains($"keywords", "etertainment"), 1).otherwise(0))
  .withColumn("politics", when(array_contains($"keywords", "politics"), 1).otherwise(0))
  .withColumn("finance", when(array_contains($"keywords", "finance"), 1).otherwise(0))

final_df.show()

final_df.withColumn("keywords", col("keywords").cast("string"))
  .coalesce(1)
  .write
  .option("header", "true")
  .csv("classified_news.csv")