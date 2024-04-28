// conduct data cleaning - remove the header row and drop unnecessary columns 
// the dataset is pre-cleaned with null values being labeled as "no data" 
// if a crucial field is missing (description / keywords), then drop the row entirely 
var CNBCDataRaw = spark.read.csv("project/cnbc_news_datase.csv").rdd
val Head = CNBCDataRaw.first()
val NoHeader = CNBCDataRaw.filter(line => !line.equals(Head))

val publisher = NoHeader.map(x => x(4))
publisher.distinct().collect().foreach(println)
// we can see that the publisher column sometimes contain unexpected values
// this is because sometimes HTML contains unexpected formats such as line breaks
// rows where publisher != CNBC are parsed incorrectly, drop them
val filteredRDD = NoHeader.filter(row => row(4) == "CNBC")

// confirm publisher data was cleaned
val new_publisher = filteredRDD.map(x => x(4))
new_publisher.distinct().collect().foreach(println)

// now remove rows without keywords as that is our main focus
val cleanedRDD = filteredRDD.filter(row => row.length > 7 && row(6) != null)

// now, keep only columns we're interested in
val formattedData = cleanedRDD.map(row => Array(row(0), row(1), row(2), row(3), row(4), row(5), row(6)))

// store the cleaned data
val csvRDD = formattedData.map(row => row.mkString(","))
csvRDD.saveAsTextFile("hdfs://nyu-dataproc-m/user/qz2190_nyu_edu/project/cnbc_cleaned.csv")
