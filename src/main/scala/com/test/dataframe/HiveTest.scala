package com.test.dataframe

import org.apache.spark.sql.SparkSession
import com.test.utils.SetWinUtils
import com.test.utils.PropertiesFileReader
import org.apache.spark.sql.functions._

object HiveTest {

  def main(args: Array[String]): Unit = {

    SetWinUtils.configureWinutils()
    val spark = SparkSession.builder().master("local")
      .appName("test")
      .config("spark.sql.warehouse.dir", f"${PropertiesFileReader.get_property("spark_warehouse_path")}")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("create database if not exists mydb")
    spark.sql("drop table mydb.emp")
    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    spark.sql(
      """create table if not exists mydb.emp
        |(id int, name string, salary int) partitioned by(dept_id int) row format delimited fields terminated by ',' stored as textFile""".stripMargin)
    val df = spark.read.option("header", "true").csv("C:\\Users\\araj34\\Documents\\Learning\\spark\\files\\emp.csv")
      .select(col("id").cast("int"), col("name"), col("salary").cast("int"), col("dept_id").cast("int"))
      .repartition(col("dept_id"))
    df.write.mode("overwrite").insertInto("mydb.emp")
    val df2 = spark.sql("select * from mydb.emp")
    df2.show()
    spark.sql("use mydb")
    spark.sql("show tables").show()
  }
}
