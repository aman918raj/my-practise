package com.test.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountDF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("word count").getOrCreate()
    val df = spark.read.text("C:\\Users\\araj34\\Documents\\Learning\\spark\\files\\emp.csv")
    val column_name = df.columns(0)
    val df_split = df.filter(!col(column_name).startsWith("id")).withColumn(column_name, split(col(column_name), ","))
    val df_split_explode = df_split.withColumn(column_name, explode(col(column_name)))
    val df_add_one = df_split_explode.withColumn("count", lit(1))
    val df_group_sum = df_add_one.groupBy(col(column_name)).agg(sum(col("count")))
    df_group_sum.show()
  }

}
