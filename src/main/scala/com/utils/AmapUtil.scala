package com.utils

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.mutable.ListBuffer

object AmapUtil {
  def getBusinessFromAMap(long: Double, lat: Double):String = {

    // https://restapi.amap.com/v3/geocode/regeo?
    // location=116.310003,39.991957&key=9f798bb4c95b76b99f8fac57524b1343
    val location = long +","+lat
    val urlStr = "https://restapi.amap.com/v3/geocode/regeo?" +
      "key=d4b993d6f575ff8bd586cef2708a8500&location="+location
    // 调用Http请求接口
    val str = HttpsUtil.get(urlStr)
    // 解析json
    val jSONObject = JSON.parseObject(str)
    val status = jSONObject.getIntValue("status")
    // 判断是否成功
    if(status ==0) return ""
    // 解析字段
    val regeocode = jSONObject.getJSONObject("regeocode")
    if(regeocode == null) return null
    val addressComponent = regeocode.getJSONObject("addressComponent")
    if(addressComponent == null) return null
    val businessAreas = addressComponent.getJSONArray("businessAreas")
    if(businessAreas == null) return null
    // 创建集合
    val result = ListBuffer[String]()
    // 循环处理数组
    for(item<-businessAreas.toArray){
      // 类型判断
      if(item.isInstanceOf[JSONObject]){
        val jSONObject = item.asInstanceOf[JSONObject]
        val name = jSONObject.getString("name")
        result.append(name)
      }
    }
    // 返回结果
    result.mkString(",")
    // 西湖,天坛
  }
}
