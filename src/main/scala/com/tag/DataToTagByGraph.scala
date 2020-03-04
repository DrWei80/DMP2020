package com.tag

import java.util.Properties

import com.utils.{JedisUtils, ReadAppDictionary, TagUtils}
import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis

object DataToTagByGraph {
  def main(args: Array[String]): Unit = {
    //设置hadoop环境变量
    System.setProperty("hadoop.home.dir","D:/hadoop-2.7.7")
    //判断参数值 如果小于2个则退出程序
    if (args.length!=5){
      println("参数不正确！")
      sys.exit()
    }

    //接受参数
    val Array(inputPath,outputPath,dictionaryPath,stopPath,day)=args
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




    //原装数据
    val srcRdd=df.filter(TagUtils.selectUser).rdd.map(row=>{

      //获取userID
      val userID=TagUtils.getAllUserID(row)
      // 返回值
      (userID,row)

    })
    //构建点
      val point=srcRdd.flatMap(x=>{
        //todo 获取Jedis连接
        val jedis: Jedis = JedisUtils.getConnection()
        val row=x._2
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
        jedis.close()
        val tags=adTag++appTag++deviceTag++keyWordTag++businessTag
        // 为了保证数据的准确性，将所有ID拼接到标签中
        val userTagsList = x._1.map((_,0)) ++ tags
        // 为了保证数据的准确性，只能其中一个Id携带标签，而其他的id不携带标签，但是类型是一样的
        x._1.map(uId=>{
          // 第一次处理数据，一定相等
          if(x._1.head.equals(uId)){
            (uId.hashCode.toLong,userTagsList)
            // 第二次循序数据处理，一定不相等
          }else{
            (uId.hashCode.toLong,List.empty)
          }
        })
      })


    // 构建边
    val edges = srcRdd.flatMap(tp=>{
      // A B C  A->B  A->C
      tp._1.map(uId=>Edge(tp._1.head.hashCode.toLong,uId.hashCode.toLong,0))
    })
    // 调用图
    val graph = Graph(point,edges)
    // 调用连通图算法，找到图中的联通分支，
    // 并取出每个联通分支中的所有点和其他分支中最小的点
    val vertices = graph.connectedComponents().vertices
    vertices.join(point).map{
      case(uId,(cmId,tags))=>{
        (cmId,tags)
      }
    }.reduceByKey((list1,list2)=>{
      // 首先变成一个集合 list1(("爱奇艺",1),("优酷",1)):::list2(("爱玩",1),("睡觉",1))
      // list(("爱奇艺",1),("优酷",1),("爱玩",1),("睡觉",1))
      (list1:::list2)
        // 分组 List("爱奇艺",List(("爱奇艺",1),("爱奇艺",1),("爱奇艺",1)))
        .groupBy(_._1)
        // 累加每个标签的Value
        .mapValues(_.map(_._2).sum)
        .toList
    }).take(20).foreach(println)
    // 关闭

    spark.stop()


  }
}
