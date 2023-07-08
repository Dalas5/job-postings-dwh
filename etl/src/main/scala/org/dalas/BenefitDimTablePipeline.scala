package org.dalas

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{explode, row_number}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.nio.file.Paths

object BenefitDimTablePipeline {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Benefit Dimension Tables ETL Pipeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    val source_path = "s3a://devtestsbucket/stagging/"
    val target_path = "s3a://devtestsbucket/presentation/"

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val benefitRawDF = spark.read
      .parquet(source_path  + "raw-df")
      .select("benefits")
      .na.drop()
      .select(explode($"benefits").alias("name"))
      .dropDuplicates()


    println("Number of benefits :" + benefitRawDF.count().toString)

    val windowSpec = Window.orderBy("name")

    val benefitDF = benefitRawDF.withColumn("key", row_number.over(windowSpec))

    benefitDF.write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .parquet(target_path  + "benefit_dim")

    benefitDF.show(20, 100, false)


  }

}
