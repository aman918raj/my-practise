package com.test.utils

import java.util.Properties


object PropertiesFileReader {

  def get_property(property_name: String): String = {

    val properties = new Properties()
    val inputStream = getClass.getClassLoader.getResourceAsStream("application.properties")
    properties.load(inputStream)
    val prop = properties.getProperty(property_name)
    return prop

  }

}
