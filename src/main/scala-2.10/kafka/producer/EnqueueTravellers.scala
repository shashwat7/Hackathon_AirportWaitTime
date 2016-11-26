package kafka.producer

import java.sql.Timestamp
import java.util.Properties

import scala.annotation.tailrec;

/**
  * Created by agarwalm on 11/26/2016.
  */
object EnqueueTravellers {

  def main(args: Array[String]): Unit = {
    val startTime =args(0)
    val broker =args(1)
    val timeGenerationPattern= args(2).toInt // Wait time before next generation (Speed of generation)
    val timeVar =args(3).toInt // Difference between previous and current traveller enqueue time

    val startOffset: Long = Timestamp.valueOf(startTime).getTime()
    val producer: kafka.javaapi.producer.Producer[Integer, String] =
      new kafka.javaapi.producer.Producer[Integer, String](new ProducerConfig(KafkaUtils.loadProp(broker)))

    KafkaUtils.generateTime(startOffset,producer,"enqueue",timeGenerationPattern,timeVar, "Traveller enqueue at: ")
  }



}
