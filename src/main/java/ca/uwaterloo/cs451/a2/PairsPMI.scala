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

class ConfPairs(args: Seq[String]) extends ScallopConf(args) with Tokenizer {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
  val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
  val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfPairs(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Pairs PMI")
    val sc = new SparkContext(conf)
    val threshold = args.threshold()

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), args.reducers())
    val totalLines = textFile.count()
    val wordCount = textFile
                      .flatMap(line => {
                        val tokens = tokenize(line)
                        if (tokens.length > 0) tokens.take(Math.min(tokens.length, 40)).distinct 
                        else List()
                      })
                      .map(word => (word, 1))
                      .reduceByKey(_ + _)
                      .collectAsMap()
    val wordCountBroadcast = sc.broadcast(wordCount)

    textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val words = tokens.take(Math.min(tokens.length, 40)).distinct
        if (words.length > 1) {
          var pairs = scala.collection.mutable.ListBuffer[(String, String)]()
          var i = 0
          var j = 0
          for (i <- 0 to words.length - 1) {
            for (j <- 0 to words.length - 1) {
              if ((i != j) && (words(i) != words(j))) {
                var pair : (String, String) = (words(i), words(j))
                pairs += pair
              }
            }
          }
          pairs.toList
        } else List()
      })
      .map(pair => (pair, 1))
      .reduceByKey(_ + _)
      .filter((m) => m._2 >= threshold)
      .map(p => {
        var left = wordCountBroadcast.value(p._1._1)
        var right = wordCountBroadcast.value(p._1._2)
       var both = p._2.toFloat
        var pmi = Math.log10((both * totalLines.toFloat) / (left * right))
        (p._1, (pmi, both.toInt))
      })
      .sortByKey()
      .map(p => "(" + p._1 + " " + p._2 + ")")
      .saveAsTextFile(args.output())
  }
}
