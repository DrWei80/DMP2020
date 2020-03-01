package com.TagTrait
//定义一个处理标签的接口
trait Tags {
  def makeTag(args:Any*):List[(String,Int)]

}
