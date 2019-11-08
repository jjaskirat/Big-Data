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

class ConfQ3(args: Seq[String]) extends ScallopConf(args) {
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

object Q3 extends Tokenizer 
{
   val log = Logger.getLogger(getClass().getName())
  
//   val outputDir = new Path(args.output())
//   FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
  
  
   def main(argv: Array[String]) {
    val args = new ConfQ3(argv)

    log.info("Input: " + args.input())
    log.info("Date : " + args.date())
//     log.info("Output: " + args.output())
//     log.info("Number of reducers: " + args.reducers())
     log.info("Text Data : " + args.text())
     log.info("Parquet Data : " + args.parquet())

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)
     
//      val count = sc.accumulator(0, "accumulator");
//      val date = sc.broadcast(args.date())
    val date = args.date()
  
     if(args.text())
     {
    val part = sc.textFile(args.input() + "/part.tbl")
      .map(line => {
        val a = line.split("\\|")
        (a(0).toInt, a(1))
      })

    val supplier = sc.textFile(args.input() + "/supplier.tbl")
      .map(line => {
        val a = line.split("\\|")
        (a(0).toInt, a(1))
      })
     
    val bPartMap = sc.broadcast(part.collectAsMap())
    val bSuppMap = sc.broadcast(supplier.collectAsMap())

    val lineitems = sc.textFile(args.input() + "/lineitem.tbl")
    lineitems
      .filter(line => {
        line.split("\\|")(10) contains date
      })
      .map(line => {
        val a = line.split("\\|")
        (a(0).toInt, (bPartMap.value(a(1).toInt), bSuppMap.value(a(2).toInt)))
      })
      .sortByKey()
      .take(20)
      .foreach(p => {
        println((p._1,p._2._1,p._2._2))
      })
       
     }
     
     
     else
     {
      val sparkSession = SparkSession.builder.getOrCreate
      val partDF = sparkSession.read.parquet(args.input() + "/part")
      val partRDD = partDF.rdd
      .map(line => {
        (line.getInt(0), line.getString(1))
      })

      val supplierDF = sparkSession.read.parquet(args.input() + "/supplier")
      val supplierRDD = supplierDF.rdd      
       .map(line => {
         (line.getInt(0), line.getString(1))
      })
     
    val bPartMap = sc.broadcast(partRDD.collectAsMap())
    val bSuppMap = sc.broadcast(supplierRDD.collectAsMap())

    val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
    val lineitemRDD = lineitemDF.rdd
      .filter(line => {
        line.getString(10) contains date
      })
      .map(line => {
        (line.getInt(0), (bPartMap.value(line.getInt(1)), bSuppMap.value(line.getInt(2))))
      })
      .sortByKey()
      .take(20)
      .foreach(p => {
        println((p._1,p._2._1,p._2._2))
      })
       
     }
   }
}
