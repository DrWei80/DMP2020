package com.utils

object TypeTransform {
  //类型转换，如果有空值则捕获异常并且返回值
  def toInt(str:String):Int={
    try{
      str.toInt
    }catch {
      case _:Exception => 0
    }
  }

  def toDouble(str:String):Double={
    try{
      str.toDouble
    }catch {
      case _:Exception => 0.0
    }
  }

}
