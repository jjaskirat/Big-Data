package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import scala.collection.mutable.ListBuffer
import org.rogach.scallop._

import scala.collection.mutable
import scala.collection.mutable._
import org.apache.spark.streaming._


class ArrivalConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

case class temptupleClass(current: Int, time_stamp: String, prev: Int) extends Serializable

object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())
  
  
  def stateSpecFunc(batchTime: Time, key: String, value: Option[Int], state: State[temptupleClass]) : Option[(String, temptupleClass)] = {
      var prev = 0
      if (state.exists()) {
        prev = state.get().current
      }
      var current = value.getOrElse(0).toInt
      var MybatchTime = batchTime.milliseconds
      if ((current >= 10) && (current >= 2*prev)){
        if (key == "goldman")
          println(s"Number of arrivals to Goldman Sachs has doubled from $prev to $current at $MybatchTime!")
        else
          println(s"Number of arrivals to Citigroup has doubled from $prev to $current at $MybatchTime!")
      }
      var temp = temptupleClass(current = current, time_stamp = "%08d".format(MybatchTime), prev = prev)
      state.update(temp)
      Some((key,temp))
  }
  
  
  def main(argv: Array[String]): Unit = {
    val args = new ArrivalConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("EventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 144)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)
    
    val goldman = List((-74.0141012, 40.7152191), (-74.013777, 40.7152275), (-74.0141027, 40.7138745), (-74.0144185, 40.7140753))
    val citigroup = List((-74.011869, 40.7217236), (-74.009867, 40.721493), (-74.010140,40.720053), (-74.012083, 40.720267))

    val output = StateSpec.function(stateSpecFunc _)
    
    val wc = stream.map(_.split(","))
      .flatMap(line => {
                var latitude = 0D
                var longitude = 0D
                var company = ""
                if (line(0) == "green")
                {   longitude = line(8).toDouble
                    latitude = line(9).toDouble   }
                else {
                    longitude = line(10).toDouble
                    latitude = line(11).toDouble 
                }
                
                if ((longitude > -74.012083 && longitude < -74.009867)      //For Citigroup
                   && (latitude > 40.720053 && latitude < 40.7217236 ))
                     company = "citigroup"
                else if ((longitude > -74.0144185 && longitude < -74.013777)
                       && (latitude > 40.7138745 && latitude < 40.7152275 ))
                     company = "goldman"
                
                if (company == "goldman") {
                     var templist = new ListBuffer[Tuple2[String,Int]]()
                     var temptuple:Tuple2[String,Int] = ("goldman",1)
                     templist += temptuple
                     templist
                }
                else if (company == "citigroup") {
                     var templist = new ListBuffer[Tuple2[String,Int]]()
                     var temptuple:Tuple2[String,Int] = ("citigroup",1)
                     templist += temptuple
                     templist
                }
                else { List() }
      })
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(10), Minutes(10))
      .mapWithState(output)//persist()

    
    //wc.saveAsTextFiles(args.output())
    var Myoutput = args.output()
    val MyRdd = wc.stateSnapshots()

    MyRdd.foreachRDD( (rdd, time) => {
                             var updatedRdd = rdd.map(line => (line._1,(line._2.current,line._2.time_stamp,line._2.prev)))
                             updatedRdd.saveAsTextFile(Myoutput+"/part-"+"%08d".format(time.milliseconds))
                       })
    
    MyRdd.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}
