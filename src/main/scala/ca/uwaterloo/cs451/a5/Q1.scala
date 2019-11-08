package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import collection.mutable.HashMap

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

class Conf(args: Seq[String]) extends ScallopConf(args) {
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

object Q1 extends Tokenizer {
   val log = Logger.getLogger(getClass().getName())
  
//   val outputDir = new Path(args.output())
//   FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
  
  
   def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date : " + args.date())
//     log.info("Output: " + args.output())
//     log.info("Number of reducers: " + args.reducers())
     log.info("Text Data : " + args.text())
     log.info("Parquet Data : " + args.parquet())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)
     

     
     val count = sc.accumulator(0, "accumulator");
//      val date = sc.broadcast(args.date())
     val date = args.date();
      
     
     if(args.text())
     {
     val textFile = sc.textFile(args.input() + "/lineitem.tbl")
     textFile.map(line=> {
       val tokens = line.split('|')
//        var arrayname = new Array[datatype](tokens.length)
       tokens
     })
     .map(line => line(10))
     .foreach(line =>
              if(line contains date)
              {
                count += 1;
              })
//      .saveAsTextFile("testoutput")
     
     println("ANSWER=" + count.value);
     }
     
     else
     {
       val sparkSession = SparkSession.builder.getOrCreate
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val textFile = lineitemDF.rdd
       
       textFile.map(line=> {
       val tokens = line.getString(10)
//        var arrayname = new Array[datatype](tokens.length)
       tokens
     })
     .foreach(line =>
              if(line contains date)
              {
                count += 1;
              })
//      .saveAsTextFile("testoutput")
     
     println("ANSWER=" + count.value);
       
     }
              
       

   }
}

