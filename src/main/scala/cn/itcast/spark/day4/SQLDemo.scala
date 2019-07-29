package cn.itcast.spark.day4

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by 1 on 2017/6/1.
  */
  object SQLDemo {

  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("SQLDemo").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val sqlContext=new SQLContext(sc)

    System.setProperty("user.name","root")
    //val personRdd=sc.textFile("hdfs://crxy0:9000/person.txt").map(line=>{
    val personRdd=sc.textFile("person.txt").map(line=>{
      val fields=line.split(",")
      Person(fields(0).toLong,fields(1),fields(2).toInt)
    })

    import sqlContext.implicits._
    val personDF= personRdd.toDF

    personDF.registerTempTable("t_person")
    sqlContext.sql("select * from t_person where age>=20 order by age desc limit 2").show

    sc.stop

  }
}
case class Person(id:Long,name:String,age:Int)