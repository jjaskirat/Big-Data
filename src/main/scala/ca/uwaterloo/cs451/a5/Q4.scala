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

class ConfQ4(args: Seq[String]) extends ScallopConf(args) {
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

object Q4 extends Tokenizer 
{
   val log = Logger.getLogger(getClass().getName())
  
//   val outputDir = new Path(args.output())
//   FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
  
  
   def main(argv: Array[String]) {
    val args = new ConfQ4(argv)

    log.info("Input: " + args.input())
    log.info("Date : " + args.date())
//     log.info("Output: " + args.output())
//     log.info("Number of reducers: " + args.reducers())
     log.info("Text Data : " + args.text())
     log.info("Parquet Data : " + args.parquet())

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)
     
//      val count = sc.accumulator(0, "accumulator");
//      val date = sc.broadcast(args.date())
    val date = args.date()

     if(args.text())
     {
    val customer = sc.textFile(args.input() + "/customer.tbl")
      .map(line => {
        val a = line.split("\\|")
        (a(0).toInt, a(3).toInt)
      })

    val nation = sc.textFile(args.input() + "/nation.tbl")
      .map(line => {
        val a = line.split("\\|")
        (a(0).toInt, a(1))
      })
    

    val bcustomer = sc.broadcast(customer.collectAsMap())
    val bnation = sc.broadcast(nation.collectAsMap())

    val lineitems = sc.textFile(args.input() + "/lineitem.tbl")
      .filter(line => {
        line.split("\\|")(10) contains date
      })
      .map(line => {
        (line.split("\\|")(0).toInt, 0)
      })

    val orders = sc.textFile(args.input() + "/orders.tbl")
    orders
      .map(line => {
        val a = line.split("\\|")
        (a(0).toInt, a(1).toInt)
      })
      .cogroup(lineitems)
      .filter(p => {
        !p._2._2.isEmpty
      })
      .map(p => {
        val nkey = bcustomer.value(p._2._1.iterator.next())
        (nkey, 1) 
      })
      .reduceByKey(_ + _)
      .sortByKey()
      .collect()
      .foreach(p => {
        println((p._1, bnation.value(p._1), p._2))
      })
     }
     
     else
     {
       
       val sparkSession = SparkSession.builder.getOrCreate
      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
      val customerRDD = customerDF.rdd
      .map(line => {
        (line.getInt(0), line.getInt(3))
      })

       val nationDF = sparkSession.read.parquet(args.input() + "/nation")
      val nationRDD = nationDF.rdd
      .map(line => {
        (line.getInt(0), line.getString(1))
      })
    

    val bcustomer = sc.broadcast(customerRDD.collectAsMap())
    val bnation = sc.broadcast(nationRDD.collectAsMap())

      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitems = lineitemDF.rdd      
       .filter(line => {
        line.getString(10) contains date
      })
      .map(line => {
        (line.getInt(0), 0)
      })

       val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      .map(line => {
        (line.getInt(0), line.getInt(1))
      })
      .cogroup(lineitems)
      .filter(p => {
        !p._2._2.isEmpty
      })
      .map(p => {
        val nkey = bcustomer.value(p._2._1.iterator.next())
        (nkey, 1) 
      })
      .reduceByKey(_ + _)
      .sortByKey()
      .collect()
      .foreach(p => {
        println((p._1, bnation.value(p._1), p._2))
      })
       
     }

  }
}





     
    
