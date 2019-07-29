package cn.itcast.spark.day2829

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 1 on 2017/5/16.
  */
object URLCount {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("URLCount").setMaster("local")
    val sc=new SparkContext(conf)
    val rdd1=sc.textFile("").map(line=>{
      val f=line.split("\t")
      (f(1),1)
    })

    val rdd2=rdd1.reduceByKey(_+_)

    val rdd3=rdd2.map(t=>{
      val url=t._1
      val host=new URL(url).getHost()
      (host,url,t._2)
    })




  }
}
