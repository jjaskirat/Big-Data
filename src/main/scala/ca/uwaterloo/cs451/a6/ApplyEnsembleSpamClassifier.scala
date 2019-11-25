package ca.uwaterloo.cs451.a6

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class Conf3(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, method)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val method = opt[String](descr = "method", required = true)
  val model = opt[String](descr = "model", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier {
	val log = Logger.getLogger(getClass().getName())

	def main(argv: Array[String]) {
		val args = new Conf3(argv)

		log.info("Input: " + args.input())
		log.info("Output: " + args.output())
		log.info("Method: " + args.method())
		log.info("Model: " + args.model())

		val conf = new SparkConf().setAppName("Apply Ensemble Spam Classifier")
		val sc = new SparkContext(conf)

		val outputDir = new Path(args.output())
		FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

		val textFile = sc.textFile(args.input())
		
		val X = sc.textFile(args.model() + "/part-00000")
		.map(line => {
			val tokens = line.substring(1, line.length()-1).split(',')
			(tokens(0).toInt, tokens(1).toDouble)
		})
		
		val Y = sc.textFile(args.model() + "/part-00001")
		.map(line => {
			val tokens = line.substring(1, line.length()-1).split(',')
			(tokens(0).toInt, tokens(1).toDouble)
		})
		
		val Britney = sc.textFile(args.model() + "/part-00002")
		.map(line => {
			val tokens = line.substring(1, line.length()-1).split(',')
			(tokens(0).toInt, tokens(1).toDouble)
		})

		val bX = sc.broadcast(X.collectAsMap())
		val bY = sc.broadcast(Y.collectAsMap())
		val bBritney = sc.broadcast(Britney.collectAsMap())
		

		def spamminess(features: Array[Int], weights: scala.collection.Map[Int, Double]) : Double = {
			var score = 0d
			features.foreach(f => if (weights.contains(f)) score += weights(f))
			score
		}

		val method = args.method()
		val tested = textFile.map(line => {
			val tokens = line.split(" ")
			val features = tokens.drop(2).map(_.toInt)
			val Xscore = spamminess(features, bX.value)
			val Yscore = spamminess(features, bY.value)
			val Britneyscore = spamminess(features, bBritney.value)
			var score = -1d
			
			if (method == "average") {
				score = (Xscore + Yscore + Britneyscore) / 3
			} else {
				if(Xscore > 0d && (Yscore > 0d || Britneyscore > 0d)) score = 1d
				if(Yscore > 0d && Britneyscore > 0d) score = 1d
			}
			var isSpam = "ham"
			if (score > 0) isSpam = "spam"
			(tokens(0), tokens(1), score, isSpam)
		})

		tested.saveAsTextFile(args.output())
	}
}
