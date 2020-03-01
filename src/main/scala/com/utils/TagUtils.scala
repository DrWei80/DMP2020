package com.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

//处理tag的工具类
object TagUtils {
  def getAnyUserID(row: Row) = {
    row match {
      case v if StringUtils.isNotBlank(v.getAs[String]("imei")) => "imei:"+v.getAs[String]("imei")
      case v if StringUtils.isNoneBlank(v.getAs[String]("mac")) => "mac:"+v.getAs[String]("mac")
      case v if StringUtils.isNoneBlank(v.getAs[String]("idfa")) => "idfa:"+v.getAs[String]("idfa")
      case v if StringUtils.isNoneBlank(v.getAs[String]("openudid")) => "openudid:"+v.getAs[String]("openudid")
      case v if StringUtils.isNoneBlank(v.getAs[String]("androidid")) => "androidid:"+v.getAs[String]("androidid")
    }
  }

  //  保证有一个ID不为空的条件
  val selectUser=
    """
      |imei !='' or mac !='' or openudid !='' or androidid !='' or idfa !=''
    """.stripMargin
}
