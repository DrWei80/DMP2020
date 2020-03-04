package com.tag

import ch.hsr.geohash.GeoHash
import com.TagTrait.Tags
import com.utils.AmapUtil
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object BusinessTag extends Tags{



  override def makeTag(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()
    val row=args(0).asInstanceOf[Row]
    val jedis=args(1).asInstanceOf[Jedis]

    /**
      * 中国的经纬度范围大约为：纬度3.86~53.55，经度73.66~135.05
      * long: String,	设备所在经度
      * lat: String,	设备所在纬度
      */
    //只要中国范围内的经纬度
    if(
      row.getAs[String]("long").toDouble>=73
      &&row.getAs[String]("long").toDouble<=135
      &&row.getAs[String]("lat").toDouble>3
      &&row.getAs[String]("lat").toDouble<54
    ){
      //todo 1.获取经纬度
      val long = row.getAs[String]("long")
      val lat = row.getAs[String]("lat")
      //todo 2.获取商圈
      val business = getBusiness(long.toDouble,lat.toDouble,jedis)
      //如果不为空则对商圈进行切分
      if(StringUtils.isNotBlank(business)){
        val str = business.split(",")
        str.foreach(t=>{
          list :+=(t,1)
        })
      }
    }
    list.distinct
  }



  def getBusiness(lat: Double, long: Double,jedis: Jedis) = {
    // 将经纬度转换成GeoHash编码
    val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(lat,long,8)
    // 去jedis数据库查询商圈
    var businessData =jedis.get(geoHash)
    // 判断是否获取到商圈
    if(businessData == null || businessData.length ==0){
      // 通过调用第三方接口 获取商圈
      businessData = AmapUtil.getBusinessFromAMap(long,lat)
      // 将商圈保存到数据库
      if(businessData !=null &&  businessData.length>0){
        jedis.set(geoHash,businessData)
      }
    }
    businessData
  }

}
