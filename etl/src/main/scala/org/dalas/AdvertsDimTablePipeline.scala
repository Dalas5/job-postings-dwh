package org.dalas

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, explode, from_unixtime, row_number, to_date}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.nio.file.Paths

object AdvertsDimTablePipeline {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Adverts Dimension Tables ETL Pipeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    val source_path = "s3a://devtestsbucket/stagging/"
    val target_path = "s3a://devtestsbucket/presentation/"

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val advertRawDF = spark.read
      .parquet(source_path + "raw-df")
      .select("adverts.*")
      .na.drop()
      .withColumn("publicationDateTime", to_date(from_unixtime($"publicationDateTime")))
      .dropDuplicates()


    println("Number of adverts :" + advertRawDF.count().toString)

    val windowSpec = Window.orderBy("id")
    val advertDF = advertRawDF.withColumn("key", row_number.over(windowSpec))

    advertDF.write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .parquet(target_path + "advert_dim")



  }

}
