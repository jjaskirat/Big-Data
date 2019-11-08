package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.util.Try
import org.apache.spark.sql.SparkSession

class ConfQ7(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  //   val output = opt[String](descr = "output path", required = true)
//   val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
//   val imc = opt[Boolean](descr = "use in-mapper combining", required = false)
  val date = opt[String](descr = "date of Select Query", required = true)
  val text = opt[Boolean](descr = "Use Text Data", required = false)
  val parquet = opt[Boolean](descr = "Use parquet Data", required = false)
  verify()
}


object Q7 extends Tokenizer
{
  val log = Logger.getLogger(getClass().getName())
  
  //   val outputDir = new Path(args.output())
//   FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
  
  def main(argv: Array[String]) {
    val args = new ConfQ7(argv)
    
    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
//     log.info("Output: " + args.output())
//     log.info("Number of reducers: " + args.reducers())
    log.info("Text Data? : " + args.text())
    log.info("Parquet Data? : " + args.parquet())
    
    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)
    
    val date = args.date().split('-').map(_.toInt)
    
    
    if(args.text())
    {
    val customer = sc.textFile(args.input() + "/customer.tbl")
    .map(line => {
      val a = line.split('|')
      (a(0).toInt, a(1))
    })
    
    val bcustomer = sc.broadcast(customer.collectAsMap())
    
    val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
    .map(line => {
      val a = line.split('|')
      (a(0).toInt, a(5).toDouble, a(6).toDouble, a(10))
    })
    .filter(p => {
      val date1 = p._4.split('-').map(_.toInt) // year-month-day // lshipdate > date
      (date1(0) > date(0)) || (date1(0) == date(0) && date1(1) > date(1)) || (date1(0) == date(0) && date1(1) == date(1) && date1(2) > date(2))
    })
    .map(p => {
      val l_extendedprice = p._2
      val l_discount = p._3
      (p._1, l_extendedprice*(1-l_discount))
    })
    
    
    val orders = sc.textFile(args.input() + "/orders.tbl")
    
    orders.
    map(line => {
      val a = line.split('|')
      (a(0).toInt, a(1).toInt, a(4), a(7))
    })
    .filter( p => {
      val date1 = p._3.split('-').map(_.toInt) // orderdate < date
      (date1(0) < date(0)) || (date1(0) == date(0) && date1(1) < date(1)) || (date1(0) == date(0) && date1(1) == date(1) && date1(2) < date(2))
    })
    .map(p => {
      (p._1, (bcustomer.value(p._2), p._3, p._4))
    })
    .cogroup(lineitem)
//     .saveAsTextFile("myOutput.txt")
    .filter(p => {
        !p._2._2.isEmpty && !p._2._1.isEmpty
      })
    .map(p => {
        val items = p._2._1.iterator.next()
        val c_name = items._1
        val l_orderkey = p._1
        val o_orderdate = items._2
        val o_shippriority = items._3
        val sum = p._2._2.foldLeft(0.0)((b,a) => b+a)
        (sum, (c_name, l_orderkey, o_orderdate, o_shippriority))
      })
      .sortByKey(false)
      .take(10)
      .foreach(p => {
        println((p._2._1, p._2._2, p._1, p._2._3, p._2._4))
      })
    }
    
    else
    {
      
      val sparkSession = SparkSession.builder.getOrCreate
      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
      val customerRDD = customerDF.rdd
      .map(line => {
      (line.getInt(0), line.getString(1))
    })
    
    val bcustomer = sc.broadcast(customerRDD.collectAsMap())
    
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      .map(line => {
      (line.getInt(0), line.getDouble(5), line.getDouble(6), line.getString(10))
    })
    .filter(p => {
      val date1 = p._4.split('-').map(_.toInt) // year-month-day // lshipdate > date
      (date1(0) > date(0)) || (date1(0) == date(0) && date1(1) > date(1)) || (date1(0) == date(0) && date1(1) == date(1) && date1(2) > date(2))
    })
    .map(p => {
      val l_extendedprice = p._2
      val l_discount = p._3
      (p._1, l_extendedprice*(1-l_discount))
    })
    
    
      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd    
    .map(line => {
      (line.getInt(0), line.getInt(1), line.getString(4), line.getInt(7))
    })
    .filter( p => {
      val date1 = p._3.split('-').map(_.toInt) // orderdate < date
      (date1(0) < date(0)) || (date1(0) == date(0) && date1(1) < date(1)) || (date1(0) == date(0) && date1(1) == date(1) && date1(2) < date(2))
    })
    .map(p => {
      (p._1, (bcustomer.value(p._2), p._3, p._4))
    })
    .cogroup(lineitemRDD)
//     .saveAsTextFile("myOutput.txt")
    .filter(p => {
        !p._2._2.isEmpty && !p._2._1.isEmpty
      })
    .map(p => {
        val items = p._2._1.iterator.next()
        val c_name = items._1
        val l_orderkey = p._1
        val o_orderdate = items._2
        val o_shippriority = items._3
        val sum = p._2._2.foldLeft(0.0)((b,a) => b+a)
        (sum, (c_name, l_orderkey, o_orderdate, o_shippriority))
      })
      .sortByKey(false)
      .take(10)
      .foreach(p => {
        println((p._2._1, p._2._2, p._1, p._2._3, p._2._4))
      })
      
    }
  }
}
