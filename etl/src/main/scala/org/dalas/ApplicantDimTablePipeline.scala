package org.dalas

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{explode, from_unixtime, row_number, to_date, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.nio.file.Paths

object ApplicantDimTablePipeline {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Applicant Dimension Tables ETL Pipeline")
      .master("local[*]")
      .getOrCreate()

    // UDF to group applicant in age ranges
    val udfAgeBin = udf((age: Int) => {
      age match {
        case a if 0 to 15 contains a => "0 to 15"
        case b if 0 to 20 contains b => "15 to 20"
        case c if 0 to 25 contains c => "20 to 25"
        case d if 0 to 30 contains d => "25 to 30"
        case e if 0 to 35 contains e => "30 to 35"
        case f if 0 to 40 contains f => "35 to 40"
        case g if 0 to 45 contains g => "40 to 45"
        case h if 0 to 50 contains h => "45 to 50"
        case i if 0 to 55 contains i => "50 to 55"
        case j if 0 to 60 contains j => "50 to 60"
        case k if 0 to 100 contains k => "60+"
        case _ => "60+"
      }
    })

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    val source_path = "s3a://devtestsbucket/stagging/"
    val target_path = "s3a://devtestsbucket/presentation/"

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val applicantRawDF = spark.read
      .parquet(source_path + "raw-df")
      .select(explode($"applicants").alias("applicant"))
      .select("applicant.*")
      .select("firstName", "lastName", "age")
      .na.drop()
      .withColumn("age_range", udfAgeBin($"age"))
      .dropDuplicates()


    println("Number of adverts :" + applicantRawDF.count().toString)

    val windowSpec = Window.orderBy("firstName", "lastName")
    val applicantDF = applicantRawDF.withColumn("key", row_number.over(windowSpec))

    applicantDF.write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .parquet(target_path + "applicant_dim")

//    applicantDF.show(20, 100, false)


  }

}
