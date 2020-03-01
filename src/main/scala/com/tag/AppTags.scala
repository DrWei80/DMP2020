package com.tag

import com.TagTrait.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object AppTags extends Tags{
  override def makeTag(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()
    //将传进来的ROW转换回ROW类型
    val row=args(0).asInstanceOf[Row]
    val appNameMap=args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]


    /**
      * appid: String,	应用 id
      * appname: String,	应用名称
      */
    val appname=row.getAs[String]("appname")
    val appid=row.getAs[String]("appid")
    //如果ROW中的AppName为空则利用字典文件获取AppName
    if(StringUtils.isNotBlank(appname)){
      list:+=("APP"+appname,1)
    }else{
      list:+=(appNameMap.value.getOrElse("appid","其他"),1)
    }

    list
  }
}
