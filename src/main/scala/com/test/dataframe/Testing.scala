package com.test.dataframe

import com.test.utils.{PropertiesFileReader, SetWinUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Testing {

  def main(args: Array[String]): Unit = {
    SetWinUtils.configureWinutils()
    val spark = SparkSession.builder().master("local")
      .appName("test")
      .config("spark.sql.warehouse.dir", f"${PropertiesFileReader.get_property("spark_warehouse_path")}")
      .enableHiveSupport()
      .getOrCreate()

    val db_name = "mydb"
    spark.sql("use mydb")
    //spark.sql("drop table test_complex_type")
    //spark.sql("create table test_complex_type(id int, schema map<string, string>, changes map<string, map<string, string>>) row format delimited fields terminated by ',' stored as textFile")

    import spark.implicits._
    val df = spark.sparkContext.parallelize(List((1, Map("test1" -> "test2"), Map("column" -> Map("address" -> "added"))))).toDF(List("id", "schema", "changes"):_*)
    val schema = df.select("schema").rdd.collect()(0)(0).asInstanceOf[Map[String, String]]
    val t = schema.getOrElse("test4", null)
    println(t)

  }

}
