package com.test.dataframe

import com.test.utils.{PropertiesFileReader, SetWinUtils}
import org.apache.spark.sql.SparkSession

object ReadFolder {

  def main(args: Array[String]): Unit = {
    SetWinUtils.configureWinutils()
    val spark = SparkSession.builder().master("local")
      .appName("test")
      .config("spark.sql.warehouse.dir", f"${PropertiesFileReader.get_property("spark_warehouse_path")}")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.read.option("header", "true").csv("C:\\Users\\<user_name>\\Documents\\Learning\\spark\\files1")
    df.show()
    println(df.rdd.getNumPartitions)
  }

}
