package com.tag

import com.TagTrait.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object AdTags extends Tags{
  override def makeTag(args: Any*): List[(String, Int)] = {
    var list=List[(String,Int)]()
    //将传进来的ROW转换回ROW类型
    val row=args(0).asInstanceOf[Row]
    //从row中读取广告的类型和名字
    /**
      * adspacetype: Int,	广告位类型（1：banner 2：插屏 3：全屏）
      * adspacetypename: String,	广告位类型名称（banner、插屏、全屏）
      */
    val adType=row.getAs[Int]("adspacetype")
    val adName=row.getAs[String]("adspacetypename")
    //todo sb需求文档就是个屎！
    //根据贴标签的规则返回一个广告标签结果
    adType match {
      case x if x>9 =>list:+=("LC"+x,1)
      case x if x>0 && x<=9 =>list:+=("LC0"+x,1)
    }
    //如果广告名称为空则进行处理
    if(StringUtils.isNotBlank(adName)){
      list:+=("LN"+adName,1)
    }

    /**
      * 渠道标签
      * adplatformproviderid: Int,	广告平台商 id	(>= 100000: rtb)
      */
    val adplatformproviderid = row.getAs[Int]("adplatformproviderid")
    if(StringUtils.isNotBlank(adplatformproviderid.toString)){
      list:+=("CN"+adplatformproviderid,1)
    }

    list
  }
}
