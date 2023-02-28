package com.test.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder


object PlayingWithPartititons {

  def createSparkSession(num_shuffle_partitions: Int): DataFrame ={
    val spark = SparkSession.builder().master("local").config("spark.sql.shuffle.partitions",num_shuffle_partitions).appName("test").getOrCreate()
    val df = spark.read.option("header", "true").csv("C:\\Users\\<user_name>\\Documents\\Learning\\spark\\files\\emp.csv")
    return df
  }
  def main(args: Array[String]) : Unit={

   val props = System.setProperty("hadoop.home.dir", "C:\\Users\\<user_name>\\hadoop")
    val df = createSparkSession(2)
    val parts = df.rdd.getNumPartitions
    println(parts)
    val partsDf = df.repartition(col("dept_id"))
    val partsDfparts = partsDf.rdd.getNumPartitions
    println(partsDfparts)


  }

}
