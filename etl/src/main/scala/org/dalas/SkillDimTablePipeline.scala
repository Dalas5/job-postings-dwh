package org.dalas

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{explode, row_number}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.nio.file.Paths

object SkillDimTablePipeline {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Skill Dimension Tables ETL Pipeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    val source_path = "s3a://devtestsbucket/stagging/"
    val target_path = "s3a://devtestsbucket/presentation/"


    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val skillRawDF = spark.read
      .parquet(source_path + "raw-df")
      .select(explode($"applicants").alias("applicant"))
      .select("applicant.skills")
      .select(explode($"skills").alias("name"))
      .na.drop()
      .dropDuplicates()


    println("Number of skills :" + skillRawDF.count().toString)

    val windowSpec = Window.orderBy("name")

    val skillDF = skillRawDF.withColumn("key", row_number.over(windowSpec))

    skillDF.write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .parquet(target_path + "skill_dim")



  }

}
