package com.etl


import java.util.Properties

import com.utils.LogUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object LogToParquet {



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
      .setMaster("local").set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val spark=SparkSession.builder().config(conf).getOrCreate()

    //配置SQLcontext的压缩方式，其实默认就是snappy
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

    //获取数据
    val data: RDD[String] =spark.sparkContext.textFile(inputPath)

    /**
      * 切分数据
      * 按逗号切分
      * 每一行有85个字段，有85个字段的留下
      */
    val line: RDD[Array[String]] =data.map(x=>{x.split(",",x.length)}).filter(_.length>=85)

    //利用动态编程方式将RDD转换成dataFrame
    //需要传入一个rddRow:RDD[Row]和一个schema:StructType
    val df=spark.sqlContext.createDataFrame(LogUtils.rddToRddRowForLog(line),LogUtils.logStructType)
    df.write.parquet(outputPath)


    //todo 将统计的结果输出成 json 格式，并输出到磁盘目录。
//    saveAsJsonToLocal(countByArea)
    spark.stop()

  }

  //将统计的结果输出成 json 格式，并输出到磁盘目录。
  def saveAsJsonToLocal(countByArea: DataFrame) = {
    countByArea.repartition(1).write.json("E:/Dr998/DMP2020_localJson")
  }




}
