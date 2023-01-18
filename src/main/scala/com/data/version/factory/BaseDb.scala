package com.data.version.factory

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

abstract class BaseDb(sparkSession: SparkSession, db_name: String) {

  def get_tables_from_db(): List[String]

  def get_schema_details_for_table(db_table_name: String): DataFrame

  def get_latest_version_of_table(hive_db_table_version: String): DataFrame

  def compare_latest_version_with_schema(db_table_name: String, schema: Map[String, String], version_table_df: DataFrame): Map[String, Map[String, String]]

  def insert_table_details()

}
