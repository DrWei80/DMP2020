package com.tag

import com.utils.{JedisUtils, ReadAppDictionary, TagUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis

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

    //todo 获取Jedis连接
    val jedis: Jedis = JedisUtils.getConnection()


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

      //处理商圈标签
      val businessTag=BusinessTag.makeTag(row,jedis)

      // 返回值
      (userID,adTag++appTag++deviceTag++keyWordTag++businessTag)
    }).reduceByKey((list1,list2)=>{
      // 首先变成一个集合 list1(("爱奇艺",1),("优酷",1)):::list2(("爱玩",1),("睡觉",1))
      // list(("爱奇艺",1),("优酷",1),("爱玩",1),("睡觉",1))
      (list1:::list2)
        // 分组 List("爱奇艺",List(("爱奇艺",1),("爱奇艺",1),("爱奇艺",1)))
        .groupBy(_._1)
        // 累加每个标签的Value
        .mapValues(_.foldLeft[Int](0)(_+_._2))
        .toList
    }).foreach(println)

    spark.stop()
    jedis.close()

  }
}
