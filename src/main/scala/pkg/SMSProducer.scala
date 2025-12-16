package pkg

import io.circe.syntax.EncoderOps
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import scala.concurrent.{Future, Promise}

class SMSProducer(producer: KafkaProducer[String, String]) {
  def sendSMSToTopic(sms: SMS, topic: String): Future[Unit] = {
    val promise = Promise[Unit]()
    println(s"sending $sms to $topic")
    val outputRecord = new ProducerRecord[String, String](topic, sms.asJson.noSpaces)
    producer.send(outputRecord, (_, exception) => {
      if (exception != null) {
        promise.failure(exception)
      } else {
        promise.success(())
      }
    })
    promise.future
  }
}
