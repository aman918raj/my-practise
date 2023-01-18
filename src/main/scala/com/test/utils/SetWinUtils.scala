package com.test.utils


object SetWinUtils {

  def configureWinutils(): Unit ={
    System.setProperty("hadoop.home.dir", PropertiesFileReader.get_property("hadoop_home_dir"))
    val cmd = f"${PropertiesFileReader.get_property("winutils_path")} chmod 777 ${PropertiesFileReader.get_property("spark_warehouse_path")}"
    Runtime.getRuntime.exec(cmd)
    val cmd2 = f"${PropertiesFileReader.get_property("winutils_path")} chmod 777 ${PropertiesFileReader.get_property("tmp_hive")}"
    val process2 = Runtime.getRuntime.exec(cmd2)
  }

}
