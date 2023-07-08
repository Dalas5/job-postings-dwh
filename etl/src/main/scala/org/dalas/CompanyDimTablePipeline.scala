package org.dalas

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{max, row_number}

import java.nio.file.Paths

object CompanyDimTablePipeline {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Company Dimension Tables ETL Pipeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    val source_path = "s3a://devtestsbucket/stagging/"
    val target_path = "s3a://devtestsbucket/presentation/"

    spark.sparkContext.setLogLevel("ERROR")

    val companyRawDF = spark.read
      .parquet(source_path + "raw-df")
      .select("company")
      .na.drop()
      .dropDuplicates()
      .withColumnRenamed("company", "name")

    println("Number of companies :" + companyRawDF.count().toString)

    val windowSpec1 = Window.orderBy("name")

    val companyDF = companyRawDF.withColumn("key", row_number.over(windowSpec1))

    companyDF.write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .parquet(target_path + "company_dim")


  }

}
