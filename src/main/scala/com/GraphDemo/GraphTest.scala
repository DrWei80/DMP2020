package com.GraphDemo

import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.sql.SparkSession

/**
  * 图计算案例
  */
object GraphTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("graph").master("local").getOrCreate()

    // 需要构建点和边
    val vertexRDD = spark.sparkContext.makeRDD(Seq(
      (1L,("梅西",34)),
      (2L,("内马尔",28)),
      (6L,("C罗",35)),
      (9L,("库蒂尼奥",37)),
      (133L,("法尔考",35)),
      (16L,("老詹",35)),
      (44L,("大姚",36)),
      (21L,("库里",30)),
      (138L,("奥尼尔",43)),
      (5L,("马云",55)),
      (7L,("马化腾",46)),
      (158L,("任正非",60))
    ))
    // 构建边
    val edgeRDD = spark.sparkContext.makeRDD(Seq(
      Edge(1L,133L,0),
      Edge(2L,133L,0),
      Edge(6L,133L,0),
      Edge(9L,133L,0),
      Edge(16L,138L,0),
      Edge(6L,138L,0),
      Edge(44L,138L,0),
      Edge(21L,138L,0),
      Edge(5L,158L,0),
      Edge(7L,158L,0)
    ))
    // 使用图计算
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD,edgeRDD)
    // 找到图中的顶点Id（最小的那个点）
    val vertices = graph.connectedComponents().vertices

    // 获取具体值
    vertices.join(vertexRDD).map{
      case(userid,(cmId,(name,age)))=>{
        (cmId,List((name,age)))
      }
    }.reduceByKey(_++_).foreach(println)
  }
}
