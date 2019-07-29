package cn.itcast.spark.day2829

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 1 on 2017/5/16.
  */

object ForeachDemo {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("ForeachDemo").setMaster("local")
    val sc=new SparkContext(conf)
    val rdd1=sc.parallelize(List(1,2,3,4,5,6,7,8,9),2)
    rdd1.foreach(println(_))
    sc.stop
  }
}
