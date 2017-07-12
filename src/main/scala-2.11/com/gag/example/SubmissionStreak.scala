package com.gag.example

import java.io.File

import org.apache.spark.sql.expressions.{UserDefinedAggregateFunction, Window}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext

class SubmissionStreak(reddit: RedditDataLoader) {
  def load(path: String): DataFrame =
    reddit.load(path)
      .select("author", "created_date")
      .distinct()
      .withColumn("prev_post_date",
        lag("created_date", 1).over(Window.partitionBy("author").orderBy("created_date")))
      .withColumn("new_streak",
        when(col("prev_post_date").isNull.or(date_sub(col("created_date"), 1) !== col("prev_post_date")), 1).otherwise(0))
      .withColumn("streak_num", sum("new_streak").over(Window.partitionBy("author").orderBy("created_date")))
      .groupBy("author", "streak_num")
      .count()
      .groupBy("author")
      .max("count")
      .orderBy(col("max(count)").desc)
      .select(col("author"), col("max(count)").as("longest_streak"))
}

object SubmissionStreak {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Reddit Submission Streak")

    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val streaks = new SubmissionStreak(new RedditDataLoader(sqlContext))
      .load(input)

    val tmpOutput = new File(System.getProperty("java.io.tmpdir") + "/submission-streak")

    streaks.coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(tmpOutput.toString)

    new File(tmpOutput + "/part-00000").renameTo(new File(output))

    sc.stop()
  }
}
