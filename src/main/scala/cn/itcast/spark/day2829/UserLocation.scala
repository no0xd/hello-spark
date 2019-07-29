package cn.itcast.spark.day2829

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 1 on 2017/5/16.
  */
object UserLocation {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("UserLocation").setMaster("local[2]")
    val sc=new SparkContext(conf)

    val mbt=sc.textFile("F:\\video\\传智播客三期视频\\day28\\练习\\bs_log").map(line=>{
      val fields=line.split(",")
      val eventType=fields(3)
      val time=fields(1)
      val timeLong =if(eventType=="1") -time.toLong else time.toLong
      (fields(0)+"_"+fields(2),timeLong)
    })

    val result=mbt.groupBy(_._1)
    println(result.collect.toBuffer)

    sc.stop
  }
}
