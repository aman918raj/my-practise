package com.test.dataframe

import com.test.utils.SetWinUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ProcessMultipleFiles {

  def main(args: Array[String]): Unit = {

    SetWinUtils.configureWinutils()
    val spark = SparkSession.builder().master("local").appName("test").getOrCreate()
    val readAllFiles = spark.sparkContext.wholeTextFiles("C:\\Users\\araj34\\Documents\\Learning\\spark\\files1", 5)
    println(readAllFiles.getNumPartitions)
    val dataRdd = readAllFiles.map(x => x._2)
    val dataMapRdd = dataRdd
      .map(line => line.split("\n"))
      .flatMap(line => line)
      .map(x => x.split(","))
      .map(x => (x(0), x(1), x(2), x(3)))
    dataMapRdd.collect().foreach(x => println(x))
    println(dataMapRdd)

    /*
    val columns = List("id", "name", "salary", "dept_id")
    import spark.implicits._
    val dataDf = dataMapRdd.toDF(columns:_*).filter(col("id") =!= "id")
    dataDf.show(5)
    println(dataDf.count())

    //val cre

     */

  }

}
