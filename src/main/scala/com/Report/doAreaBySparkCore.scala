package com.Report

import com.util.logUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object doAreaBySparkCore {
  def main(args: Array[String]): Unit = {
    //设置hadoop环境变量
    System.setProperty("hadoop.home.dir","D:/hadoop-2.7.7")
    //判断参数值 如果小于2个则退出程序
    if (args.length!=2){
      println("参数不正确！")
      sys.exit()
    }

    //接受参数
    val Array(inputPath,outputPath)=args
    //设置序列化
    val conf=new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val spark=SparkSession.builder().config(conf).getOrCreate()

    //配置SQLcontext的压缩方式，其实默认就是snappy
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

    //读取parquet文件
    val df: DataFrame = spark.read.parquet(inputPath)
    val filedData=df.rdd.map(x=>{
      //获取省份与城市信息
      val provincename=x.getAs[String]("provincename")
      val cityname=x.getAs[String]("cityname")
      //封装数据为一个元组((String,String),List(....))
      ((provincename,cityname),logUtil.calculateField(x))
    })
    filedData.reduceByKey{case (a,b)=>{
      a.zip(b).map(x=>{x._1+x._2})
    }}.foreach(println)
    spark.stop()
  }
}
