package com.tag

import com.TagTrait.Tags
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object KeyWordTags extends Tags{
  override def makeTag(args: Any*): List[(String, Int)] = {
    var list= List[(String, Int)]()
    val row=args(0).asInstanceOf[Row]
    //停用词库
    val stopWordMap = args(1).asInstanceOf[Broadcast[collection.Map[String, Int]]]

    /**
      * keywords: String,	关键字
      */
    val keyWords=row.getAs[String]("keywords").split("\\|")
    /**
      * 根据关键字标签规则过滤
      * 关键字个数不能少于 3 个字符，且不能超过 8 个字符
      * 不需要停用词库中的词
      */
    keyWords.filter(x=>{
      x.length>=3 && x.length<=8 && stopWordMap.value.contains(x)
    })
      .foreach(x=>{
        list:+=("K"+x,1)
      })
    list
  }
}
