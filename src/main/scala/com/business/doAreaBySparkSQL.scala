package com.business

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object doAreaBySparkSQL {
  //todo 处理地域分布
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

    //读取parquet文件
    val df: DataFrame = spark.read.parquet(inputPath)

    df.createOrReplaceTempView("doArea")
    //todo 按照省市分布，求满足条件的总数(参照计算逻辑编写SQL)
    /**
      * 作为条件的日志字段有
      * requestmode
      * processnode
      * iseffective
      * isbilling
      * isbid
      * iswin
      * adorderid
      * 特殊的
      * winprice/1000
      * adpayment/1000
      *
      */
    val result=spark.sql(
      """
        |select provincename,cityname,
        |sum(case when requestmode=1 and processnode>=1 then 1 else 0 end) original_requests,
        |sum(case when requestmode=1 and processnode>=2 then 1 else 0 end) effective_requests,
        |sum(case when requestmode=1 and processnode=3 then 1 else 0 end) ad_requests,
        |sum(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) join_bid,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end) bid_success,
        |sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) show_nums,
        |sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) click_nums,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then winprice/1000 else 0 end) dsp_ad_consume,
        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment/1000 else 0 end) dsp_ad_cost
        |from doArea
        |group by provincename,cityname
      """.stripMargin)
    result.show()

    //将数据结果储存
//    result.write.partitionBy("provincename","cityname").save(outputPath)
    spark.stop()
  }

}
