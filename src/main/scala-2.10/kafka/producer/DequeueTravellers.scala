package kafka.producer

import java.sql.Timestamp

/**
  * Created by root on 26/11/16.
  */
object DequeueTravellers {
  def main(args: Array[String]): Unit = {
    val startTime =args(0)
    val broker =args(1)
    val timeGenerationPattern= args(2).toInt // Wait time before next generation (Speed of generation)
    val timeVar =args(3).toInt // Difference between previous and current traveller enqueue time

    val startOffset: Long = Timestamp.valueOf(startTime).getTime()
    val producer: kafka.javaapi.producer.Producer[Integer, String] =
      new kafka.javaapi.producer.Producer[Integer, String](new ProducerConfig(KafkaUtils.loadProp(broker)))

    KafkaUtils.generateTime(startOffset,producer,"dequeue",timeGenerationPattern,timeVar, "Traveller dequeue at: ")
  }
}
