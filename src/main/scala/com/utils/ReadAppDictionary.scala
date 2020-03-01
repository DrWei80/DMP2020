package com.utils

import org.apache.spark.sql.SparkSession

object ReadAppDictionary {
  def broadDictionary(spark: SparkSession, dictionaryPath: String) = {
    //读取字典文件 取出APP的名字和IP 以KV形式广播
    val dictionaryFile=spark.sparkContext.textFile(dictionaryPath)
      .map(_.split("\\s+",-1))
      .filter(_.length>=5)
      .map(x=>{
        //其中K是appIP,V是appName
        (x(4),x(1))
      })
      .collectAsMap()
    spark.sparkContext.broadcast(dictionaryFile)
  }

}
