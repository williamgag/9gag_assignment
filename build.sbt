name := "william-9gag-test"

version := "1.0"

scalaVersion := "2.11.11"

val versions = new {
  val spark = "1.6.1"
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % versions.spark,
  "org.apache.spark" %% "spark-sql" % versions.spark,
  "org.apache.spark" %% "spark-hive" % versions.spark,
  "com.databricks" %% "spark-csv" % "1.5.0"
)
