package cn.itcast.spark.day2829

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by 1 on 2017/5/16.
  */
object URLCount2 {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("URLCount").setMaster("local[2]")
    val sc=new SparkContext(conf)

    val rdd1=sc.textFile("F:\\video\\传智播客三期视频\\day29\\itcast.log").map(line=>{
      val f=line.split("\t")
      (f(1),1)
    })

    val rdd2=rdd1.reduceByKey(_+_)

    val rdd3=rdd2.map(t=>{
      val url=t._1
      val host=new URL(url).getHost()
      (host,(url,t._2))
    })

    val ins=rdd3.map(_._1).distinct.collect

    val hostPartitioner=new HostPartitioner(ins)

    val rdd4: RDD[(String, (String, Int))] = rdd3.partitionBy(hostPartitioner).mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(3).iterator
    })

    rdd4.saveAsTextFile("F:\\test3")




    //rdd3.repartition(3).saveAsTextFile("F:\\test")

  }
}
//决定了数据到哪个分区里面
class HostPartitioner(ins: Array[String]) extends Partitioner {

  val parMap = new mutable.HashMap[String, Int]()

  var count = 0

  for (i <- ins) {
    parMap += (i -> count)
    count += 1
  }

  override def numPartitions: Int = ins.length

  override def getPartition(key: Any): Int = {
    //parMap(key.toString)
    parMap.getOrElse(key.toString,0)
  }
}


