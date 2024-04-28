// for both columns, keep only the columns we need and drop all rows with missing data
val df_raw = spark.read.option("header", "true").csv("final_project/data_ingest/cnbc_news_datase.csv")
val df_withNa = df_raw.select("published_at", "keywords")

val df2_raw = spark.read.option("header", "true").csv("final_project/data_ingest/huffpost_news_data.csv")
val df2_withNa = df2_raw.select("published_at", "tags")
val df = df_withNa.na.drop(how="all")
val df2 = df2_withNa.na.drop(how="all")

// parsing the date column
val df_adjusted = df.withColumn("published_at", substring($"published_at", 1, 10))
val df_parsedDate = df_adjusted.withColumn("published_at", to_date($"published_at", "yyyy-MM-dd"))
val df2_parsedDate = df2.withColumn("published_at", to_date($"published_at", "yyyy-MM-dd"))

// create a year month column
val df_withYearMonth = df_parsedDate.withColumn("year_month", date_format($"published_at", "yyyy-MM"))
val df2_withYearMonth = df2_parsedDate.withColumn("year_month", date_format($"published_at", "yyyy-MM"))

// spliting keywords column of each csv into array
val df_parsedKeywords = df_withYearMonth.withColumn("keywords", split($"keywords", ", "))
val df2_parsedKeywords = df2_withYearMonth.withColumn("keywords", split($"tags", ","))

// text transform: trim and turn to lower case
val df_transformedKeywords = df_parsedKeywords.withColumn("keywords", transform($"keywords", f => lower(trim(f))))
val df2_transformedKeywords = df2_parsedKeywords.withColumn("keywords", transform($"keywords", f => lower(trim(f))))

val df_keep = df_transformedKeywords.select("published_at", "keywords", "year_month")
val df2_keep = df2_transformedKeywords.select("published_at", "keywords", "year_month")
val df_cleaned = df_keep.na.drop(how="all")
val df2_cleaned = df2_keep.na.drop(how="all")

// combine 
val df_combined = df_cleaned.union(df2_cleaned).select("published_at", "keywords", "year_month")

val cleaned_df = df_combined.withColumn("keywords", transform(col("keywords"), x => lower(x)))

// create a column that indicates if a news article is an entertainment article
// utilized help of chatgpt to format these complex regex queries 
val final_df = cleaned_df
    .withColumn("entertainment", when(
      expr("array_contains(transform(keywords, x -> x rlike '.*entertainment.*'), true)"), 1
    ).otherwise(0))
    .withColumn("health", when(
      expr("array_contains(transform(keywords, x -> x rlike '.*health.*'), true)"), 1
    ).otherwise(0))
  .withColumn("politics", when(
      expr("array_contains(transform(keywords, x -> x rlike '.*politics.*'), true)"), 1
    ).otherwise(0))
  .withColumn("finance", when(
      expr("array_contains(transform(keywords, x -> x rlike '.*finance.*'), true)"), 1
    ).otherwise(0))
  .withColumn("technology", when(
    expr("array_contains(transform(keywords, x -> x rlike '.*technology.*'), true)"), 1
  ).otherwise(0))

final_df.show()

final_df.withColumn("keywords", col("keywords").cast("string"))
  .coalesce(1)
  .write
  .option("header", "true")
  .csv("classified_news")