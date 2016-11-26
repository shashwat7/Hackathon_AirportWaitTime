package kafka.producer

import java.sql.Timestamp
import java.util.Properties

import scala.annotation.tailrec

/**
  * Created by root on 26/11/16.
  */
object KafkaUtils {

  def pushToKafka(producer :kafka.javaapi.producer.Producer[Integer, String],topic: String, time: String) =
    producer.send(new KeyedMessage[Integer, String](topic, time))

  def loadProp(broker :String) ={
    val props : Properties = new Properties()
    props.put("serializer.class", "kafka.serializer.StringEncoder")
    props.put("metadata.broker.list", broker)
    props
  }

  @tailrec
  def generateTime(startTime: Long,
                   producer :kafka.javaapi.producer.Producer[Integer, String],
                   topicName:String,
                   timeGenerationPattern :Int,
                   timeVar:Int,
                   message: String): Long = {
    val diff: Long = timeVar // Difference between previous and current time
    val generatedTimeInLong = startTime + (Math.random() * diff).toLong
    val newTime = new Timestamp(generatedTimeInLong)
    println(message + newTime)

    // Push to Kafka
    KafkaUtils.pushToKafka(producer,topicName, newTime.toString)

    // Wait before generating next message
    val waitTime = timeGenerationPattern + (Math.random() * 100).toInt
    Thread.sleep(waitTime)

    // Generate next message
    generateTime(generatedTimeInLong,producer,topicName,timeGenerationPattern,timeVar,message)
  }

}
