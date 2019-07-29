package cn.itcast.spark.day2829

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by 1 on 2017/5/16.
  */
object ADVURLCount {
  def main(args: Array[String]): Unit = {
    val arr:Array[String]=Array("java.itcast.cn","php.itcast.cn","net.itcast.cn")

    val conf = new SparkConf().setAppName("ADVURLCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("F:\\video\\传智播客三期视频\\day29\\itcast.log").map(line => {
      val f = line.split("\t")
      (f(1), 1)
    })

    val rdd2 = rdd1.reduceByKey(_ + _)

    val rdd3 = rdd2.map(t => {
      val url = t._1
      val host = new URL(url).getHost()
      (host, url, t._2)
    })

    //    println(rdd3.collect.toBuffer)




   /* val rddjava = rdd3.filter(_._1 == "java.itcast.cn")
    val sortedjava = rddjava.sortBy(_._3, false).take(3)

    val rddphp = rdd3.filter(_._1 == "php.itcast.cn")
    val sortedphp = rddphp.sortBy(_._3, false).take(3)

    val sortedRdd = rddjava.sortBy(_._3, false).take(3)

    println(rddjava.collect.toBuffer)
    println(sortedjava.toBuffer)
    println(rddphp.collect.toBuffer)
    println(sortedphp.toBuffer)

    println(sortedRdd.toBuffer)*/


    for(ins <- arr){
      val rdd = rdd3.filter(_._1 == ins)
      val sortedRdd = rdd.sortBy(_._3, false).take(3)
      //写入到关系型数据库中
      println(rdd.collect.toBuffer)
      println(sortedRdd.toBuffer)
    }

    sc.stop

  }
}
