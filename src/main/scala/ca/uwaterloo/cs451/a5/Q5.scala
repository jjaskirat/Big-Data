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

class ConfQ5(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input)
  val input = opt[String](descr = "input path", required = true)
//   val output = opt[String](descr = "output path", required = true)
//   val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
//   val imc = opt[Boolean](descr = "use in-mapper combining", required = false)
//   val date = opt[String](descr = "date of Select Query", required = true)
  val text = opt[Boolean](descr = "Use Text Data", required = false)
  val parquet = opt[Boolean](descr = "Use parquet Data", required = false)
  verify()
}

object Q5 extends Tokenizer 
{
   val log = Logger.getLogger(getClass().getName())
  
//   val outputDir = new Path(args.output())
//   FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
  
  
   def main(argv: Array[String]) {
    val args = new ConfQ5(argv)

    log.info("Input: " + args.input())
//     log.info("Date : " + args.date())
//     log.info("Output: " + args.output())
//     log.info("Number of reducers: " + args.reducers())
     log.info("Text Data : " + args.text())
     log.info("Parquet Data : " + args.parquet())

    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)
     
//      val count = sc.accumulator(0, "accumulator");
//      val date = sc.broadcast(args.date())
//     val date = args.date()

     
     if(args.text())
     {
    val customer = sc.textFile(args.input() + "/customer.tbl")
      .map(line => {
        val a = line.split("\\|")
        (a(0).toInt, a(3).toInt)
      })
     .map(pair => {
       if(pair._2 == 3){
         (pair._1, "CANADA")
       }
       else if(pair._2 == 24)
       {
         (pair._1, "US")
       }
       else
       {
         (pair._1, "NA")
       }
     })
     
    val nation = sc.textFile(args.input() + "/nation.tbl")
     .map(line => {
       val a = line.split('|')
       (a(0).toInt, a(1))
     })
   
    val lineitems = sc.textFile(args.input() + "/lineitem.tbl")
     .map(line => {
       val a = line.split('|')
       (a(0).toInt, a(10).substring(0,7))
     })
    
    val bcustomer = sc.broadcast(customer.collectAsMap())
    val bnation = sc.broadcast(nation.collectAsMap())
    
     
    val orders = sc.textFile(args.input() + "/orders.tbl")
     .map(line => {
       val a = line.split('|')
       (a(0).toInt, a(1).toInt)
     })
     .cogroup(lineitems)
     .filter(p => {
       !p._2._2.isEmpty && !p._2._1.isEmpty
     })       
     .filter(p => {
       val temp = bcustomer.value(p._2._1.iterator.next())
       if(temp == "US" || temp =="CANADA") true
       else false
     })
    .flatMap(p => {
      val temp = bcustomer.value(p._2._1.iterator.next())
//       while(p._2._2.iterator.hasNext)
//       {
//         ((temp, p._2._2.iterator.next()), 1)
//       }
      p._2._2.map(d => ((temp, d), 1)).toList
    })
    .reduceByKey(_+_)
    .sortByKey()
    .collect()
    .foreach(p => {
        println((p._1._1, p._1._2, p._2))
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
     .map(pair => {
       if(pair._2 == 3){
         (pair._1, "CANADA")
       }
       else if(pair._2 == 24)
       {
         (pair._1, "US")
       }
       else
       {
         (pair._1, "NA")
       }
     })
     
      val nationDF = sparkSession.read.parquet(args.input() + "/nation")
      val nationRDD = nationDF.rdd
     .map(line => {
       (line.getInt(0), line.getString(1))
     })
   
      val lineitemsDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitems = lineitemsDF.rdd
     .map(line => {
       (line.getInt(0), line.getString(10).substring(0,7))
     })
    
    val bcustomer = sc.broadcast(customerRDD.collectAsMap())
    val bnation = sc.broadcast(nationRDD.collectAsMap())
    
     
      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd     
       .map(line => {
       (line.getInt(0), line.getInt(1))
     })
     .cogroup(lineitems)
     .filter(p => {
       !p._2._2.isEmpty && !p._2._1.isEmpty
     })       
     .filter(p => {
       val temp = bcustomer.value(p._2._1.iterator.next())
       if(temp == "US" || temp =="CANADA") true
       else false
     })
    .flatMap(p => {
      val temp = bcustomer.value(p._2._1.iterator.next())
//       while(p._2._2.iterator.hasNext)
//       {
//         ((temp, p._2._2.iterator.next()), 1)
//       }
      p._2._2.map(d => ((temp, d), 1)).toList
    })
    .reduceByKey(_+_)
    .sortByKey()
    .collect()
    .foreach(p => {
        println((p._1._1, p._1._2, p._2))
    })
       
     }
  }
} 
       
     
