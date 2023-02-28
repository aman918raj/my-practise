package com.test.dataframe

import com.test.utils.{PropertiesFileReader, SetWinUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.commons.io.FilenameUtils

object UpdatePartitionHive {

  def main(args: Array[String]): Unit = {

    SetWinUtils.configureWinutils()
    val spark = SparkSession.builder().master("local")
      .appName("test")
      .config("spark.sql.warehouse.dir", f"${PropertiesFileReader.get_property("spark_warehouse_path")}")
      .enableHiveSupport()
      .getOrCreate()

    //get the file name in string
    val df = spark.sql("select * from mydb.emp").withColumn("file_name", input_file_name())
    val file_name = df.filter(col("id") === 1).select("file_name").first()(0)
    println(file_name)
    val file_path = FilenameUtils.getPath(file_name.toString)
    println(file_path)

    //write the contents of that file
    val df2 = df
      .filter(col("file_name") === file_name)
      .drop("file_name")
      .withColumn("salary", when(col("name") === "A", 1000).otherwise(col("salary")))
    df2.write.mode("overwrite").format("csv").save("C:\\Users\\<user_name>\\Documents\\Learning\\spark\\emp_out")

    //change the value of salary when name = "A"
    val df3 = spark.read.csv("C:\\Users\\<user_name>\\Documents\\Learning\\spark\\emp_out").withColumn("file_name", input_file_name())
    val file_name2 = df3.select("file_name").first()(0)
    val exact_file_name = FilenameUtils.getName(file_name2.toString)
    println(exact_file_name)

    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)

    val srcPath = new Path(s"C:\\Users\\<user_name>\\Documents\\Learning\\spark\\emp_out\\$exact_file_name")
    val destPath = new Path(file_path)

    //hdfs.copyToLocalFile(srcPath, destPath)
    hdfs.delete(new Path(file_name.toString), true)

    //val df4 = df3.withColumn("salary", when(col("name") === "A", 1000).otherwise(col("salary")))
    //df4.show()
    //spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    //df4.write.mode("overwrite").insertInto("mydb.emp")

  }

}
