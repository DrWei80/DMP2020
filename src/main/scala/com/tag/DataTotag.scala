package com.tag

import com.utils.{ReadAppDictionary, TagUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataTotag {
  def main(args: Array[String]): Unit = {
    //设置hadoop环境变量
    System.setProperty("hadoop.home.dir","D:/hadoop-2.7.7")
    //判断参数值 如果小于2个则退出程序
    if (args.length!=4){
      println("参数不正确！")
      sys.exit()
    }

    //接受参数
    val Array(inputPath,outputPath,dictionaryPath,stopPath)=args
    //设置序列化
    val conf=new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local").set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val spark=SparkSession.builder().config(conf).getOrCreate()

    //配置SQLcontext的压缩方式，其实默认就是snappy
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

    //读取parquet文件
    val df: DataFrame = spark.read.parquet(inputPath)

    //读取字典文件并且广播
    val dictionaryBroadcast=ReadAppDictionary.broadDictionary(spark,dictionaryPath)
    // 读取停用词库
    val stopWordMap = spark.sparkContext.textFile(stopPath).map((_,0)).collectAsMap()
    // 广播
    val stopWordBroadCast = spark.sparkContext.broadcast(stopWordMap)

    //给数据打标签
    df.filter(TagUtils.selectUser).rdd.map(row=>{
      //获取userID
      val userID=TagUtils.getAnyUserID(row)

      //处理广告标签
      val adTag=AdTags.makeTag(row)

      //处理APP标签,需要读取字典文件
      val appTag=AppTags.makeTag(row,dictionaryBroadcast)

      //处理设备标签
      val deviceTag=DeviceTags.makeTag(row)

      //处理关键字标签
      val keyWordTag=KeyWordTags.makeTag(row,stopWordBroadCast)
    })

    spark.stop()

  }
}
