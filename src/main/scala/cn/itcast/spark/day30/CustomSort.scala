package cn.itcast.spark.day30

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object OrderContext {
  //implicit object GirlOrdering extends Ordering[Girl] {
  implicit val girlOrdering=new Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
      if (x.faceValue > y.faceValue) 1
      else if (x.faceValue == y.faceValue) {
        if (x.age > y.age) -1 else 1
      } else -1
    }
  }

}

/**
  * Created by 1 on 2017/5/20.
  */

//name faceValue age
object CustomSort {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("ADVURLCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(("yuihatano", 90, 28, 1), ("angelababy", 90, 27, 2), ("jujingyi", 95, 22, 3)))

    import OrderContext.girlOrdering
    val rdd2: RDD[(String, Int, Int, Int)] = rdd1.sortBy(x => Girl(x._2, x._3), false)
    println(rdd2.collect.toBuffer)

    sc.stop
  }
}

/*//第一种方式
case class Girl(faceValue:Int,age:Int) extends Ordered[Girl] with Serializable{
  override def compare(that: Girl): Int = {
    if(this.faceValue==that.faceValue){
      that.age-this.age
    }else{
      this.faceValue-that.faceValue
    }
  }
}*/

case class Girl(faceValue: Int, age: Int) extends Serializable {

}