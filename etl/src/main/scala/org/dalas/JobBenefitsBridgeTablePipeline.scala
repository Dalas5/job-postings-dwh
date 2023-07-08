package org.dalas

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{explode, row_number}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.nio.file.Paths

object JobBenefitsBridgeTablePipeline {
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

    import spark.implicits._

    val jobBenefitsBridgeRawDF = spark.read
      .parquet(source_path + "raw-df")
      .select($"id", explode($"benefits").alias("benefit"))
      // .select(explode($"benefits").alias("benefit"))
      .na.drop()
      .dropDuplicates()

    val benefitDimDF = spark.read
      .parquet(target_path + "benefit_dim")

    val jobDimDF = spark.read
      .parquet(target_path + "job_dim")

    val jobBenefitsBridgeDF = jobBenefitsBridgeRawDF
      .join(benefitDimDF, benefitDimDF("name") === jobBenefitsBridgeRawDF("benefit"), "inner")
      .withColumnRenamed("key", "benefit_key")
      .withColumnRenamed("name", "benefit_name")
      .join(jobDimDF, jobDimDF("id") === jobBenefitsBridgeRawDF("id"), "inner")
      .withColumnRenamed("key", "job_key")
      .select("job_key", "benefit_key")

    println("Number of jobs-benefit correlations :" + jobBenefitsBridgeDF.count().toString)


    jobBenefitsBridgeDF.write
          .option("compression", "snappy")
          .mode(SaveMode.Overwrite)
          .parquet(target_path + "job_benefit_bridge")

    // jobBenefitsBridgeDF.show(20, 100, false)


  }

}
