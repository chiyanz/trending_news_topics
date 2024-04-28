// read data
var df = spark.read.option("header", "true").csv("final_project/data_ingest/cnbc_news_datase.csv")

val df_parsed = df.withColumn("timestamp", to_timestamp($"published_at", "yyyy-MM-dd'T'HH:mm:ssZ"))
  .withColumn("year", year($"timestamp"))
  .withColumn("month", month($"timestamp"))
  .withColumn("day", dayofmonth($"timestamp"))

// calculate mean mode and median
// Year
val year_stats = df_parsed.groupBy("year").count().orderBy(desc("count"))
val median_year = df_parsed.stat.approxQuantile("year", Array(0.5), 0.01)
// Month
val month_mean = df_parsed.select(mean($"month").alias("mean_month"))
val median_month = df_parsed.stat.approxQuantile("month", Array(0.5), 0.01)
val month_mode = df_parsed.groupBy("month").count().orderBy(desc("count")).limit(1)
// Day
val day_mean = df_parsed.select(mean($"day").alias("mean_day"))
val median_day = df_parsed.stat.approxQuantile("day", Array(0.5), 0.01)
val day_mode = df_parsed.groupBy("day").count().orderBy(desc("count")).limit(1)

// Show output
println("Year Mode: ")
year_stats.show(1)
println(s"Median Year: ${median_year(0)}")

month_mean.show()
println(s"Median Month: ${median_month(0)}")
println("Month Mode: ")
month_mode.show()

day_mean.show()
println(s"Median Day: ${median_day(0)}")
println("Day Mode: ")
day_mode.show()


// part 2: data cleaning
// Date formatting
// Text formatting (removing spaces, extra characters, making all caps or all lowercase for normalization / future joining)
// Create a binary column based on the condition of another column.

// read in the two datasets
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
val cleaned_df = df_combined.na.drop(how="all")

// create a column that indicates if a news article is an entertainment article
val final_df = cleaned_df.withColumn("Entertainment", when(array_contains($"keywords", "Entertainment"), 1).otherwise(0))
final_df.show()

df.write
  .option("header", "true")
  .csv("hdfs://nyu-dataproc-m/user/{$USER}_nyu_edu/project/combined_cleaned.csv")