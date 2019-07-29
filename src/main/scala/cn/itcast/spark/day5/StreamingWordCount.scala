package cn.itcast.spark.day5

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by 1 on 2017/6/10.
  */
object StreamingWordCount {
  def main(args: Array[String]): Unit = {
    //StreamingContext
    val conf=new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val ssc=new StreamingContext(sc,Seconds(5))
    //接收数据
    val ds= ssc.socketTextStream("crxy0", 8888)
    //DStream 是一个特殊的RDD
    val result = ds.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
    //存储
    /*result.mapPartitions(it=>{
      val connection
      it.map()
    })*/
    //打印结果
    result.print

    ssc.start
    ssc.awaitTermination

  }
}
