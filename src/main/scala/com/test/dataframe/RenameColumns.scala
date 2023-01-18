package com.test.dataframe

import org.apache.spark.sql.SparkSession

object RenameColumns {

  def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    val df = spark.read.option("header", "true").csv("C:\\Users\\araj34\\Documents\\Learning\\spark\\files\\emp.csv")
    val columnName = df.columns
    val newColumnName = columnName.map(x => f"${x}_${columnName.indexOf(x)}")
    val newColumnNameDf = df.toDF(newColumnName:_*)
    newColumnNameDf.show()
  }

}
