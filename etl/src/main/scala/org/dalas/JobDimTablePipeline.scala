package org.dalas

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.nio.file.Paths

object JobDimTablePipeline {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Job Dimension Tables ETL Pipeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    val source_path = "s3a://devtestsbucket/stagging/"
    val target_path = "s3a://devtestsbucket/presentation/"

    spark.sparkContext.setLogLevel("ERROR")

    val jobRawDF = spark.read
      .parquet(source_path + "raw-df")
      .select("id", "city", "title", "sector")
      .na.drop()
      .dropDuplicates()

    println("Number of jobs :" + jobRawDF.count().toString)

    val windowSpec = Window.orderBy("id")

    val jobDF = jobRawDF.withColumn("key", row_number.over(windowSpec))

    jobDF.write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .parquet(target_path + "job_dim")


  }

}
