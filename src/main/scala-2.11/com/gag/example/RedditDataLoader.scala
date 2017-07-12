package com.gag.example

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, LongType, TimestampType}

class RedditDataLoader(sqlContext: SQLContext) {
  def load(path: String, clean: Boolean = true): DataFrame = {
    val data = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("parserLib", "UNIVOCITY")
      .load(path)

    if (clean)
      data
        .filter(
          col("subreddit").isNotNull
            .and(col("author").isNotNull)
            .and(col("author") !== "[deleted]")
        )
        .withColumn("created_date", col("created_utc").cast(LongType).cast(TimestampType).cast(DateType))
        .where(col("created_date").isNotNull)
        .where(col("subreddit") !== "")
    else
      data
  }
}
