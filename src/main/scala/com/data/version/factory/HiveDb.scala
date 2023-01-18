package com.data.version.factory

import org.apache.spark.sql.functions.{col, concat_ws, lower, max}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

class HiveDb(spark: SparkSession, db_name: String) extends BaseDb(spark, db_name) {


  def get_tables_from_db(): List[String] ={
    /*
    1. Getting list of tables from a database
    2. creating a list of db_name.table_name
     */
    spark.sql(s"use $db_name")
    val df = spark.sql("show tables")
      .withColumn("db_table_name", concat_ws(".", col("database"), col("tableName")))
    val selectDf = df.select(col("db_table_name"))
    val lstDbTables = selectDf.rdd.map(x => x(0).toString).collect().toList
    return lstDbTables
  }

  def get_schema_details_for_table(db_table_name: String): DataFrame ={

    spark.sql(s"refresh table $db_table_name")
    val describeTableDf = spark.sql(s"describe formatted $db_table_name").drop("comment")

    //table type
    val tableType = describeTableDf.filter(lower(col("col_name")) === "type").rdd.collect()(0)(1).toString

    //table location
    val tableLocation = describeTableDf.filter(lower(col("col_name")) === "location").rdd.collect()(0)(1).toString

    //create dataframe from spark catalog
    val dtypesDf = spark.catalog.listColumns(s"$db_table_name")

    //partition information
    val isPartitionedDf = dtypesDf.filter(col("isPartition") === true)
    var partitionedColumns = ""
    if (isPartitionedDf.count() > 0) {
      partitionedColumns = isPartitionedDf.select("name").rdd.map(x => x(0).toString).collect().toList.mkString(",")
    }

    //column name & data type
    val colDTypeDf = dtypesDf.select("name", "dataType")
    val mapColumnsDataType = colDTypeDf.rdd.map(x => (x(0).toString, x(1).toString)).collectAsMap()

    import spark.implicits._
    val dfCols = List(db_table_name,"schema", "patitioned_column", "table_type", "location")
    val createDf = spark.sparkContext.parallelize(List((db_table_name,mapColumnsDataType, partitionedColumns, tableType, tableLocation))).toDF(dfCols:_*)

    return createDf
  }

  def get_latest_version_of_table(hive_db_table_version: String): DataFrame = {

    val df = spark.table(hive_db_table_version)
    val windowSpec = Window.partitionBy("db_table_name")
    val tableRecordDF = df.withColumn("max_revision", max(col("revision").over(windowSpec)))
      .filter(col("revision") === col("max_revision"))
      .drop("max_revision")

    return tableRecordDF
  }

  def compare_latest_version_with_schema(db_table_name: String, schema: Map[String, String], version_table_df: DataFrame): Map[String, Map[String, String]] = {

    val versionTableSchema: Map[String, String] = version_table_df.filter(col("db_table_name") === db_table_name)
      .select("schema").rdd.collect()(0)(0).asInstanceOf[Map[String, String]]

    val schemaVersionDiff = (schema.toSet diff versionTableSchema.toSet).toMap

    val diffMap = scala.collection.mutable.Map[String, Map[String, String]]()

    schemaVersionDiff.foreach{x =>

      val columnPresent = versionTableSchema.getOrElse(x._1, null)
      if (columnPresent == null){
        diffMap.put("column", Map(columnPresent -> "added"))

      } else if (columnPresent != null && !x._2.equals(columnPresent)){
        diffMap.put("column", Map(columnPresent -> "data type changed"))
      }

    }

    val versionSchemaDiff = (versionTableSchema.toSet diff schema.toSet).toMap

    versionSchemaDiff.foreach{x =>

      val columnPresent = schema.getOrElse(x._1, null)
      if (columnPresent == null){
        diffMap.put("column", Map(columnPresent -> "deleted"))
      }
    }
    diffMap.toMap

  }

  def insert_table_details(): Unit = {

  }
}
