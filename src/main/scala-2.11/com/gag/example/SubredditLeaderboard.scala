package com.gag.example

import java.io.File

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{DateType, LongType, TimestampType}
import org.apache.spark.sql.functions._

class SubredditLeaderboard(reddit: RedditDataLoader) {
  def load(path: String): DataFrame = {
    reddit.load(path)
      .select("subreddit", "author", "created_date")
      .groupBy("subreddit", "author", "created_date")
      .count()
      .orderBy(col("subreddit"), desc("count"))
  }
}

object SubredditLeaderboard {
  def main(args: Array[String]): Unit = {
    val input = args(0)
    val output = args(1)

    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Reddit Subreddit Leaderboard")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data = new SubredditLeaderboard(new RedditDataLoader(sqlContext))
        .load(input)

    val tmpOutput = new File(System.getProperty("java.io.tmpdir") + "/subreddit-leaderboard")

    data.coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(tmpOutput.toString)

    new File(tmpOutput + "/part-00000").renameTo(new File(output))

    sc.stop()
  }
}
