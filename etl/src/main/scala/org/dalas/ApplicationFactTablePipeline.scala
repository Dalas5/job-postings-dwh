package org.dalas

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{explode, from_unixtime, row_number, to_date}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.nio.file.Paths

object ApplicationFactTablePipeline {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Application Factless Fact Table ETL Pipeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "******************")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

    val source_path = "s3a://devtestsbucket/stagging/"
    val target_path = "s3a://devtestsbucket/presentation/"

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val applicationRawDF = spark.read
      .parquet(source_path + "raw-df")
      .select(
        $"id".as("job_id"),
        $"adverts.publicationDateTime".as("publicationDateTime"),
        $"adverts.id".as("advert_id"),
        $"company".as("company_name"),
        explode($"applicants").alias("applicant"))
      .na.drop()
      .dropDuplicates()
      .withColumn("publicationDate_key", to_date(from_unixtime($"publicationDateTime")))
      .select(        "*",
        "applicant.firstName",
        "applicant.lastName",
        "applicant.age")
      .drop("applicant", "publicationDateTime")


    val companyDimDF = spark.read
      .parquet(target_path + "company_dim")
      .select("key", "name")

    val jobDimDF = spark.read
      .parquet(target_path + "job_dim")
      .select("key", "id")

    val applicantDimDF = spark.read
      .parquet(target_path + "applicant_dim")
      .select("firstName", "lastName", "age", "key")

    val advertDimDF = spark.read
      .parquet(target_path + "advert_dim")
      .select("key", "id")

    val skillDimDF = spark.read
      .parquet(target_path + "skill_dim")


    val applicationDFwithJobId = applicationRawDF
      .join(companyDimDF, companyDimDF("name") === applicationRawDF("company_name"))
      .drop("name", "company_name")
      .withColumnRenamed("key", "company_key")
      .join(jobDimDF, jobDimDF("id") === applicationRawDF("job_id"))
//      .drop("id", "job_id")
      .drop("id")
      .withColumnRenamed("key", "job_key")
      .join(applicantDimDF, applicantDimDF("firstName") === applicationRawDF("firstName")
        && applicantDimDF("lastName") === applicationRawDF("lastName")
          && applicantDimDF("age") === applicationRawDF("age"))
      .withColumnRenamed("key", "applicant_key")
      .drop("firstName", "lastName", "age")
      .join(advertDimDF, advertDimDF("id") === applicationRawDF("advert_id"))
      .withColumnRenamed("key", "advert_key")
      .drop("id", "advert_id")

    val windowSpec = Window.orderBy(
      "publicationDate_key",
      "company_key",
      "job_key", "applicant_key",
      "advert_key")

    val applicantComlpetedDFwithJobId = applicationDFwithJobId.withColumn("application_key", row_number.over(windowSpec))
    val applicantComlpetedDF = applicantComlpetedDFwithJobId.drop("job_id").withColumnRenamed("application_key", "key")


    // Subpipeline for ApplicationSkillset Bridge table
    val applicationRawSkillArrayLkpDF = spark.read
      .parquet(source_path + "raw-df")
      .select(
        $"id".as("job_id"),
        explode($"applicants").alias("applicant"))
      .na.drop()

    val applicationRawSkillLkpDF = applicationRawSkillArrayLkpDF.select($"*",
      explode($"applicant.skills").alias("skill"))
      .dropDuplicates()
      .drop("applicant")

    val joinedApplicationRawSkillLkpDF = applicationRawSkillLkpDF
      .join(skillDimDF, skillDimDF("name") === applicationRawSkillLkpDF("skill"))
      .withColumnRenamed("key", "skill_key")

    val applicationSkillsetRawDF = applicantComlpetedDFwithJobId
      .join(joinedApplicationRawSkillLkpDF,
        joinedApplicationRawSkillLkpDF("job_id") === applicantComlpetedDFwithJobId("job_id"))
      .select("application_key", "skill_key")

    applicationSkillsetRawDF.write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .parquet(target_path + "application_skillset_bridge")

    applicantComlpetedDF.write
      .option("compression", "snappy")
      .mode(SaveMode.Overwrite)
      .parquet(target_path + "application_fact")


  }

}
