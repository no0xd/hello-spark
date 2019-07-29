package cn.itcast.spark.day2829

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 1 on 2017/5/14.
  */
object WC {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("WC")
    val sc: SparkContext = new SparkContext(conf)
    sc.textFile(args(0)).flatMap(_.split("\t")).map((_,1)).reduceByKey(_+_,1).sortBy(_._2,false).saveAsTextFile(args(1))
    sc.stop
  }
}

