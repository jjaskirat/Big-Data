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

class ConfQ2(args: Seq[String]) extends ScallopConf(args) {
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

object Q2 extends Tokenizer 
{
   val log = Logger.getLogger(getClass().getName())
  
//   val outputDir = new Path(args.output())
//   FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
  
  
   def main(argv: Array[String]) {
    val args = new ConfQ2(argv)

    log.info("Input: " + args.input())
    log.info("Date : " + args.date())
//     log.info("Output: " + args.output())
//     log.info("Number of reducers: " + args.reducers())
     log.info("Text Data : " + args.text())
     log.info("Parquet Data : " + args.parquet())

    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)
     
//      val count = sc.accumulator(0, "accumulator");
//      val date = sc.broadcast(args.date())
     val date = args.date();
     
     if(args.text())
     {
     val orders = sc.textFile(args.input() + "/orders.tbl")
  			.map(line => (line.split("\\|")(0).toInt, line.split("\\|")(6)))
  		
  		val lineitem = sc.textFile(args.input() + "/lineitem.tbl")
  			.map(line => (line.split("\\|")(0).toInt, line.split("\\|")(10)))
  			.filter(_._2.contains(date))
  			.cogroup(orders)
  			.filter(_._2._1.size != 0)
  			.sortByKey()
  			.take(20)
  			.map(p => (p._2._2.head, p._1.toLong))
        .foreach(println)
       
     }
     
     else
     {
       val sparkSession = SparkSession.builder.getOrCreate
      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      val orders = ordersRDD
        .map(line => (line.getInt(0), line.getString(6)))
      
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val lineitem = lineitemRDD
        .map(line => (line.getInt(0), line.getString(10)))
        .filter(_._2.contains(date))
        .cogroup(orders)
        .filter(_._2._1.size != 0)
        .sortByKey()
        .take(20)
        .map(p => (p._2._2.head, p._1.toLong))
        .foreach(println)
     }
   }
}
