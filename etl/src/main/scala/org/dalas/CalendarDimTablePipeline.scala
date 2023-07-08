package org.dalas

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}

import java.nio.file.Paths

object CalendarDimTablePipeline {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Calendar Dimension Tables ETL Pipeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    val source_path = "s3a://devtestsbucket/stagging/"
    val target_path = "s3a://devtestsbucket/presentation/"

    spark.sparkContext.setLogLevel("ERROR")

    val calendarSchema = new StructType()
      .add("date_key", IntegerType, nullable = true)
      .add("full_date", DateType, nullable = true)
      .add("day_of_week", IntegerType, nullable = true)
      .add("day_num_in_month", IntegerType, nullable = true)
      .add("day_name", StringType, nullable = true)
      .add("week_num_in_year", IntegerType, nullable = true)
      .add("month", IntegerType, nullable = true)
      .add("month_name", StringType, nullable = true)
      .add("quarter", IntegerType, nullable = true)
      .add("year", IntegerType, nullable = true)
      .add("month_end_flag", StringType, nullable = true)
      .add("same_day_year_ago", DateType, nullable = true)

    val calendarRawDF = spark.read
      .format("com.crealytics.spark.excel")
      .schema(calendarSchema)
      .option("header", "true")
      .load(source_path + "datedim.xls")

    calendarRawDF.printSchema()
    calendarRawDF.show()

    calendarRawDF.write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .parquet(target_path + "calendar_dim")
  }

}
