package com.test.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ConevertSingleToMultipleColumns {

  def main(args: Array[String]): Unit ={

    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    val df = spark.read.text("C:\\Users\\<user_name>\\Documents\\Learning\\spark\\files\\emp.csv")
    val columnName = df.columns(0)
    val splitDf = df.withColumn("splited_col", split(col(columnName), ","))
    val realColumnNames = splitDf.first().toSeq.toList.head.toString.split(",")
    val renamedDF = realColumnNames.foldLeft(splitDf){(splitDf, x) => splitDf.withColumn(x, col("splited_col")(realColumnNames.indexOf(x)))}
    renamedDF.show()
  }

}
