package org.dalas

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types._

import java.nio.file.Paths

object PostsStagging {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Job Posting Stagging")
      .master("local[*]")
      .getOrCreate()


    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    val source_path = "s3a://technical-dev-test/raw/jobs/"
    val target_path = "s3a://devtestsbucket/stagging/"

    spark.sparkContext.setLogLevel("ERROR")

    val jobPostSchema = new StructType()
      .add("id", StringType, nullable = true)
      .add("adverts",
        new StructType()
          .add("id", StringType, nullable = true)
          .add("activeDays", IntegerType, nullable = true)
          .add("applyUrl", StringType, nullable = true)
          .add("publicationDateTime", StringType, nullable = true)
          .add("status", StringType, nullable = true)
        , nullable = true)
      .add("benefits", ArrayType(StringType), nullable = true)
      .add("company", StringType, nullable = true)
      .add("sector", StringType, nullable = true)
      .add("title", StringType, nullable = true)
      .add("city", StringType, nullable = true)
      .add("applicants", ArrayType(
        new StructType()
          .add("firstName", StringType, nullable = true)
          .add("lastName", StringType, nullable = true)
          .add("skills", ArrayType(StringType), nullable = true)
          .add("age", IntegerType, nullable = true)
          .add("applicationDate", StringType, nullable = true))
        , nullable = true)



    val postingsRawDF = spark.read
      .schema(jobPostSchema)
      .json(source_path)

    postingsRawDF.printSchema()

    postingsRawDF.show(5, 100, true)

    postingsRawDF.describe().show(10, 100, false)

    println("Row count: " + postingsRawDF.count().toString)


    postingsRawDF.write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .parquet(target_path + "raw-df")

    spark.stop()

  }
}
