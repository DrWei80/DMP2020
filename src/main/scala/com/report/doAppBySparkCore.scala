package com.report


import com.util.logUtil
import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object doAppBySparkCore {
  def main(args: Array[String]): Unit = {
    //设置hadoop环境变量
    System.setProperty("hadoop.home.dir","D:/hadoop-2.7.7")
    //判断参数值 如果小于2个则退出程序
    if (args.length!=3){
      println("参数不正确！")
      sys.exit()
    }
    //接受参数
    val Array(inputPath,outputPath,dictionaryPath)=args
    //设置序列化
    val conf=new SparkConf()
      .setMaster("local")
      .setAppName(this.getClass.getName)
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark=SparkSession.builder().config(conf).getOrCreate()
    //配置SQLcontext的压缩方式，其实默认就是snappy
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

    //读取字典文件 取出APP的名字和IP 以KV形式广播

    val dictionaryFile=spark.sparkContext.textFile(dictionaryPath)
      .map(_.split("\\s+",-1))
      .filter(_.length>=5)
      .map(x=>{
        //其中K是appIP,V是appName
        (x(4),x(1))
      })
      .collectAsMap()
    val dictionaryBroadcast=spark.sparkContext.broadcast(dictionaryFile)

    //读取数据
    val df=spark.read.parquet(inputPath)
    //处理数据 按媒体分析
    val filedData=df.rdd.map(x=>{
    //统一用appName分组
      var appName=x.getAs[String]("appname")
      //如果找不到appName则根据appIP从广播变量中获取appname
      if(StringUtils.isBlank(appName)){
        appName=dictionaryBroadcast.value.getOrElse(x.getAs[String]("appid"),"未知软件")
      }
      (appName,logUtil.calculateField(x))
    })
    //聚合
    filedData.reduceByKey{case(a,b)=>{
      a.zip(b).map(x=>{x._1+x._2})
    }}.foreach(println)

    spark.stop()
  }
}
