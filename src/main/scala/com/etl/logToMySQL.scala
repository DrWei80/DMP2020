package com.etl

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object logToMySQL {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "D:/hadoop-2.7.7")
    //判断参数值 如果小于2个则退出程序
    if (args.length != 2) {
      println("参数不正确！")
      sys.exit()
    }

    //接受参数
    val Array(inputPath, outputPath) = args
    //设置序列化
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val spark = SparkSession.builder().config(conf).getOrCreate()

    //配置SQLcontext的压缩方式，其实默认就是snappy
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    //读取parquet文件
    val df: DataFrame = spark.read.parquet(inputPath)

    //将统计的结果输出成 json 格式，并输出到磁盘目录。
    //{"ct":943,"provincename":"内蒙古自治区","cityname":"阿拉善盟"}
    df.createOrReplaceTempView("countByArea")
    val countByArea=spark.sql(
      """
        |select count(1) as ct,provincename,cityname
        |from countByArea
        |group by provincename,cityname
      """.stripMargin)


    //加载JDBC
    val properties = new Properties()
    properties.load(this.getClass.getClassLoader.getResourceAsStream("jdbc.properties"))
    //存入MySQL
    countByArea.write.mode(SaveMode.Append)
        .jdbc(properties.getProperty("url"),properties.getProperty("tableName"),properties)
    spark.stop()

  }
}
