package ca.uwaterloo.cs451.a6

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.math.exp

class ConfTrain(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, model)
    val input = opt[String](descr = "input path", required = true)
    val model = opt[String](descr = "model path", required = true)
    val shuffle = opt[Boolean](descr = "shuffle", required = false)
    verify()
}

object TrainSpamClassifier {
    val log = Logger.getLogger(getClass().getName())

    def main(argv: Array[String]) {
        val args = new ConfTrain(argv)

        log.info("Input: " + args.input())
        log.info("Model: " + args.model())

        val conf = new SparkConf().setAppName("Train Spam Classifier")
        val sc = new SparkContext(conf)

        val outputDir = new Path(args.model())
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

        var textFile = sc.textFile(args.input())

        val w = scala.collection.mutable.Map[Int, Double]()

        def spamminess(features: Array[Int]): Double = {
            var score = 0d
            features.foreach(f =>
                if (w.contains(f)) score += w(f))
            score
        }

        val delta = 0.002
        
        if (args.shuffle()) {
			textFile = textFile
				.map(line => (scala.util.Random.nextInt(), line))
				.sortByKey()
				.map(p => p._2)
        }

        val trained = textFile.map(line => {
                val tokens = line.split(" ")
                val docid = tokens(0)
                var isSpam = 0d
                if(tokens(1) == "spam") isSpam = 1d
                val features = tokens.drop(2).map(_.toInt)
                    (0, (docid, isSpam, features))
            })
            .groupByKey(1)
            .flatMap(p => {
                p._2.foreach(item => {
                    val features = item._3
                    val isSpam = item._2
                    val score = spamminess(features)
                    val prob = 1.0 / (1 + exp(-score))
                    features.foreach(f => {
                        if (w.contains(f)) {
                            w(f) += (isSpam - prob) * delta
                        } else {
                            w(f) = (isSpam - prob) * delta
                        }
                    })
                })
                w
            })

        trained.saveAsTextFile(args.model())
    }
}
