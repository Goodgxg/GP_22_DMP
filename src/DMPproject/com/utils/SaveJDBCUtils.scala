package com.utils

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode}

object SaveJDBCUtils {
  def saveDataToMysql(df:DataFrame,tableName:String)={
    val load = ConfigFactory.load("application.conf")
    val pro = new Properties()
    pro.put("user",load.getString("jdbc.user"))
    pro.put("password",load.getString("jdbc.password"))
    val url = load.getString("jdbc.url")
    df.write.mode(SaveMode.Append)
      .jdbc(url,tableName,pro)

  }

}
