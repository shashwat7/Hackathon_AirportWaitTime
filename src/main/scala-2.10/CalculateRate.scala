import java.sql.Timestamp
import java.util.TimeZone

import com.datastax.spark.connector.streaming._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
import org.joda.time.format.DateTimeFormat
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

/**
  * Created by root on 26/11/16.
  */
object CalculateRate {
  type OptionMap = Map[Symbol, Any]
  type time = (Int,Int,Int,Int,Int)
  val FORMATTER = DateTimeFormat.forPattern("dd-MMM-yyyy HH:mm:ss")

  val usage = """
    Usage: [-t table] [-k keyspace] [-kb kafka_broker] [-kt kafka_broker] [-c spark_conf]
    -t === type of copy - table to table/keyspace to kespace
    -o === file containing keyspaces and table names
    -f === configuration file path
              """

  val REF_CONF = "conf/config.properties"

  def numberOfPeopleInTheQueue(enqueue: DStream[(time, Int)], dequeue: DStream[(time, Int)]) = {
    enqueue.join(dequeue).map{case (time, (enqCount, deqCount)) => (time, enqCount - deqCount)}
  }

  def calculateCountPerMinute(queue: DStream[String]): DStream[(time, Int)] = {
    queue.map{t =>
      val timestamp = Timestamp.valueOf(t)
      val year = timestamp.getYear + 1900
      val month = timestamp.getMonth + 1
      val day = timestamp.getDate
      val hour = timestamp.getHours
      val minutes = timestamp.getMinutes
      ((year, month, day, hour, minutes), 1)
    }.reduceByKeyAndWindow((x: Int, y: Int) => x+y, Seconds(300))
  }

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println(usage)
      System.exit(1)
    }
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))

    val argList = args.toList
    val options = populateOptionMap(Map(), argList)

    val cassandra_keyspace = options.get('k).get.asInstanceOf[String]
    val cassandra_table = options.get('t).get.asInstanceOf[String]
    val kafka_broker = options.get('kb).get.asInstanceOf[String]
    val kafka_enqueue_topic = Set(options.get('eq).get.asInstanceOf[String])
    val kafka_dequeue_topic = Set(options.get('dq).get.asInstanceOf[String])
    val spark_conf = options.get('c)

    val conf = spark_conf match {
      case Some(x: String) => ConfigReader.load(x)
      case _               => ConfigReader.load(REF_CONF)
    }

    val sparkConf = getSparkConf(conf)
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(60))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafka_broker, "auto.offset.reset" -> "smallest")
    val enqueue = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafka_enqueue_topic)
    val dequeue: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, kafka_dequeue_topic)

    saveQueueConsumptionRate(cassandra_keyspace, dequeue.map(_._2))

    // Find number of people in the queue at any given minute
    numberOfPeopleInTheQueue(calculateCountPerMinute(enqueue.map(_._2)), calculateCountPerMinute(dequeue.map(_._2)))
      .map{case (time, count) => TimeVsSize(time._1, time._2, time._3, time._4, time._5, count)}
      .saveToCassandra(cassandra_keyspace, "time_vs_size")

    ssc.start()
    ssc.awaitTermination()
  }

  def saveQueueConsumptionRate(cassandra_keyspace: String, dequeue: DStream[String]) = {
    dequeue.map{t =>
      val timestamp = Timestamp.valueOf(t)
      val year = timestamp.getYear + 1900
      val month = timestamp.getMonth + 1
      val day = timestamp.getDate
      val hour = timestamp.getHours
      val minutes = timestamp.getMinutes
      ((year, month, day, hour, minutes), 1)
    }.reduceByKeyAndWindow((x: Int, y: Int) => x+y, Seconds(300))
      .map{case ((year, month, day, hour, minutes), rate) => TimeVsRate(year, month, day, hour, minutes, rate.toDouble)}
      .saveToCassandra(cassandra_keyspace, "time_vs_rate")
  }

  def getSparkConf(configMap: Map[String, String]): SparkConf = {
    val sparkConf = new SparkConf().setAppName("QueueStats").setMaster(configMap.get("master").get)
    val conf = configMap.filter(x => x._1.startsWith("SPARKCONF_")).map(x => (x._1.drop(10), x._2))
    //conf.foreach(x => println(x._1 + "===>" +  x._2))
    conf.foreach(x => sparkConf.set(x._1, x._2))
    sparkConf
  }

  def populateOptionMap(map: OptionMap, list: List[String]): OptionMap = {
    list match {
      case Nil => map
      case "-k" :: value :: tail =>
        populateOptionMap(map ++ Map('k -> value.toString), tail)
      case "-t" :: value :: tail =>
        populateOptionMap(map ++ Map('t -> value.toString), tail)
      case "-kb" :: value :: tail =>
        populateOptionMap(map ++ Map('kb -> value.toString), tail)
      case "-eq" :: value :: tail =>
        populateOptionMap(map ++ Map('eq -> value.toString), tail)
      case "-dq" :: value :: tail =>
        populateOptionMap(map ++ Map('dq -> value.toString), tail)
      case "-c" :: value :: tail =>
        populateOptionMap(map ++ Map('c -> value.toString), tail)
      case option :: tail =>
        sys.error("Unknown option " + option)
        sys.exit(1)
    }
  }

}
