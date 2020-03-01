package com.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
//专门用于处理该Log的工具类
object LogUtils {

  //处理字段并根据计算逻辑返还结果，适用于sparkCore写法
  def calculateField(x: Row) = {
    /**
      *  作为条件的日志字段有
      *  requestmode
      *  processnode
      *  iseffective
      *  isbilling
      *  isbid
      *  iswin
      *  adorderid
      *  特殊的
      *  winprice/1000
      *  adpayment/1000
      */
    //读取字段数据
    val requestmode = x.getAs[Int]("requestmode")
    val processnode = x.getAs[Int]("processnode")
    val iseffective = x.getAs[Int]("iseffective")
    val isbilling = x.getAs[Int]("isbilling")
    val isbid = x.getAs[Int]("isbid")
    val iswin = x.getAs[Int]("iswin")
    val adorderid = x.getAs[Int]("adorderid")
    //两个特殊的
    val winprice = x.getAs[Double]("winprice")
    val adpayment = x.getAs[Double]("adpayment")

    //根据字段数据返还结果
    val original_requests=if(requestmode==1 && processnode>=1){1}else{0}
    val effective_requests=if(requestmode==1 && processnode>=2){1}else{0}
    val ad_requests=if(requestmode==1 && processnode==3){1}else{0}

    val join_bid=if(iseffective==1 && isbilling==1 && isbid==1){1}else{0}
    val bid_success=if(iseffective==1 && isbilling==1 && iswin==1 && adorderid!=0){1}else{0}
    val show_nums=if(requestmode==2 && iseffective==1){1}else{0}
    val click_nums=if(requestmode==3 && iseffective==1){1}else{0}

    val dsp_ad_consume=if(iseffective==1 && isbilling==1 && iswin==1){winprice/1000}else{0}
    val dsp_ad_cost=if(iseffective==1 && isbilling==1 && iswin==1){adpayment/1000}else{0}
    List(original_requests,effective_requests,ad_requests,join_bid,bid_success,show_nums,click_nums,dsp_ad_consume,dsp_ad_cost)
  }

  // Rdd转换为RddROW 共85个字段
  def rddToRddRowForLog(line: RDD[Array[String]]):  RDD[Row] = {
    val rddRow: RDD[Row] =line.map(arr=>{
      Row(
        arr(0),
        TypeTransform.toInt(arr(1)),
        TypeTransform.toInt(arr(2)),
        TypeTransform.toInt(arr(3)),
        TypeTransform.toInt(arr(4)),
        arr(5),
        arr(6),
        TypeTransform.toInt(arr(7)),
        TypeTransform.toInt(arr(8)),
        TypeTransform.toDouble(arr(9)),
        TypeTransform.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        TypeTransform.toInt(arr(17)),
        arr(18),
        arr(19),
        TypeTransform.toInt(arr(20)),
        TypeTransform.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        TypeTransform.toInt(arr(26)),
        arr(27),
        TypeTransform.toInt(arr(28)),
        arr(29),
        TypeTransform.toInt(arr(30)),
        TypeTransform.toInt(arr(31)),
        TypeTransform.toInt(arr(32)),
        arr(33),
        TypeTransform.toInt(arr(34)),
        TypeTransform.toInt(arr(35)),
        TypeTransform.toInt(arr(36)),
        arr(37),
        TypeTransform.toInt(arr(38)),
        TypeTransform.toInt(arr(39)),
        TypeTransform.toDouble(arr(40)),
        TypeTransform.toDouble(arr(41)),
        TypeTransform.toInt(arr(42)),
        arr(43),
        TypeTransform.toDouble(arr(44)),
        TypeTransform.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        TypeTransform.toInt(arr(57)),
        TypeTransform.toDouble(arr(58)),
        TypeTransform.toInt(arr(59)),
        TypeTransform.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        TypeTransform.toInt(arr(73)),
        TypeTransform.toDouble(arr(74)),
        TypeTransform.toDouble(arr(75)),
        TypeTransform.toDouble(arr(76)),
        TypeTransform.toDouble(arr(77)),
        TypeTransform.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        TypeTransform.toInt(arr(84)))
    })
    rddRow
  }

  //定义一个该Log的StructType 共85个字段
  val logStructType=StructType(
    Seq(
      StructField("sessionid",StringType,true),
      StructField("advertisersid",IntegerType,true),
      StructField("adorderid",IntegerType,true),
      StructField("adcreativeid",IntegerType,true),
      StructField("adplatformproviderid",IntegerType,true),
      StructField("sdkversion",StringType,true),
      StructField("adplatformkey",StringType,true),
      StructField("putinmodeltype",IntegerType,true),
      StructField("requestmode",IntegerType,true),
      StructField("adprice",DoubleType,true),
      StructField("adppprice",DoubleType,true),
      StructField("requestdate",StringType,true),
      StructField("ip",StringType,true),
      StructField("appid",StringType,true),
      StructField("appname",StringType,true),
      StructField("uuid",StringType,true),
      StructField("device",StringType,true),
      StructField("client",IntegerType,true),
      StructField("osversion",StringType,true),
      StructField("density",StringType,true),
      StructField("pw",IntegerType,true),
      StructField("ph",IntegerType,true),
      StructField("long",StringType,true),
      StructField("lat",StringType,true),
      StructField("provincename",StringType,true),
      StructField("cityname",StringType,true),
      StructField("ispid",IntegerType,true),
      StructField("ispname",StringType,true),
      StructField("networkmannerid",IntegerType,true),
      StructField("networkmannername",StringType,true),
      StructField("iseffective",IntegerType,true),
      StructField("isbilling",IntegerType,true),
      StructField("adspacetype",IntegerType,true),
      StructField("adspacetypename",StringType,true),
      StructField("devicetype",IntegerType,true),
      StructField("processnode",IntegerType,true),
      StructField("apptype",IntegerType,true),
      StructField("district",StringType,true),
      StructField("paymode",IntegerType,true),
      StructField("isbid",IntegerType,true),
      StructField("bidprice",DoubleType,true),
      StructField("winprice",DoubleType,true),
      StructField("iswin",IntegerType,true),
      StructField("cur",StringType,true),
      StructField("rate",DoubleType,true),
      StructField("cnywinprice",DoubleType,true),
      StructField("imei",StringType,true),
      StructField("mac",StringType,true),
      StructField("idfa",StringType,true),
      StructField("openudid",StringType,true),
      StructField("androidid",StringType,true),
      StructField("rtbprovince",StringType,true),
      StructField("rtbcity",StringType,true),
      StructField("rtbdistrict",StringType,true),
      StructField("rtbstreet",StringType,true),
      StructField("storeurl",StringType,true),
      StructField("realip",StringType,true),
      StructField("isqualityapp",IntegerType,true),
      StructField("bidfloor",DoubleType,true),
      StructField("aw",IntegerType,true),
      StructField("ah",IntegerType,true),
      StructField("imeimd5",StringType,true),
      StructField("macmd5",StringType,true),
      StructField("idfamd5",StringType,true),
      StructField("openudidmd5",StringType,true),
      StructField("androididmd5",StringType,true),
      StructField("imeisha1",StringType,true),
      StructField("macsha1",StringType,true),
      StructField("idfasha1",StringType,true),
      StructField("openudidsha1",StringType,true),
      StructField("androididsha1",StringType,true),
      StructField("uuidunknow",StringType,true),
      StructField("userid",StringType,true),
      StructField("iptype",IntegerType,true),
      StructField("initbidprice",DoubleType,true),
      StructField("adpayment",DoubleType,true),
      StructField("agentrate",DoubleType,true),
      StructField("lomarkrate",DoubleType,true),
      StructField("adxrate",DoubleType,true),
      StructField("title",StringType,true),
      StructField("keywords",StringType,true),
      StructField("tagid",StringType,true),
      StructField("callbackdate",StringType,true),
      StructField("channelid",StringType,true),
      StructField("mediatype",IntegerType,true)
    )
  )



}
