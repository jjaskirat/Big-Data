/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class ConfStripesPMI(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
  verify()
}

object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfStripesPMI(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Compute Bigram Relative Frequency Stripes")
    val sc = new SparkContext(conf)
   val threshold = args.threshold()

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), args.reducers())
   
    val totalLines = textFile.count()

    var wordCount = textFile.flatMap(line => {
     var tokens = tokenize(line)
     
     if(tokens.length >0) tokens.take(Math.min(40, tokens.length)).distinct
     else List()
    })
   .map(word => (word,1))
   .reduceByKey(_+_)
   .collectAsMap()
   
   val wordCountBroadcast = sc.broadcast(wordCount)
   
   textFile.flatMap(line => {
    val tokens = tokenize(line)
    val uniqueTokens = tokens.take(Math.min(40, tokens.length)).distinct
    if(uniqueTokens.length > 0){
    var pairs = scala.collection.mutable.ListBuffer[(String, String)]()
    for(i <- 0 to uniqueTokens.length-1) {
     for(j <- 0 to uniqueTokens.length-1) {
      if(i!=j && uniqueTokens(i) != uniqueTokens(j)) {
       var pair: (String,String) = (uniqueTokens(i), uniqueTokens(j))
       pairs += pair
      }
     }
    }
     pairs.toList
    }
    else List()
   })
   .map(pair => (pair._1, Map(pair._2 -> 1.0)))
   .reduceByKey((Smap1, Smap2)  => {
    Smap1 ++ Smap2.map{case(key,value) => key -> (value + Smap1.getOrElse(key, 0.0)) }
   })
   .filter((pair) => pair._2.foldLeft(0.0)(_+_._2) >= threshold)
   .map(pair => {
    var both = pair._2.foldLeft(0.0)(_+_._2)
    val right = pair._2.map{case(key,value) => key + "->" + ((totalLines.toDouble * both) / (wordCountBroadcast.value(pair._1) * wordCountBroadcast.value(key)))}
    (pair._1, right)
   })
   .map(pair => "(" + pair._1 + ",{ " + (pair._2 mkString ", ") + " }" +")")
   .saveAsTextFile(args.output())
  }
}
   
   


     
