import com.outworkers.phantom.dsl.{CreateQueryOps, context}

import java.time.Duration
import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters.SeqHasAsJava
import io.circe.parser.decode
import io.circe.syntax.EncoderOps
import pkg.{LinkChecker, MyDBProvider, SMS, SMSChecker}

object Main extends MyDBProvider {
  val googleApiKey: String = sys.env.getOrElse("GOOGLE_API_KEY", "fake")
  val optInNumber: String = sys.env.getOrElse("OPT_IN_NUMBER", "123")

  println("Waiting for casandra...")
  Thread.sleep(30000)
  println("Done waiting")
  Await.result(
    db.clientOptedIn.create.ifNotExists().future(),
    10.seconds
  )

  val linkChecker = new LinkChecker(googleApiKey)
  val smsChecker = new SMSChecker(db, linkChecker)

  val bootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

  val consumerProps = new Properties()
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "sms-phishing-filter-consumer-group")
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val consumer = new KafkaConsumer[String, String](consumerProps)
  consumer.subscribe(List("sms-input").asJava)

  val producerProps = new Properties()
  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val producer = new KafkaProducer[String, String](producerProps)

  def handleOptIn(sms: SMS): Future[RecordMetadata] = {
    if (sms.message == "START") {
      db.clientOptedIn.add(sms.sender).flatMap { _ =>
        sendSMSToTopic(sms, "sms-output")
        val ackSMS = SMS(sender = optInNumber, recipient = sms.sender, message = "Usługa filtrowania phishingu włączona")
        sendSMSToTopic(ackSMS, "sms-output")
      }
    } else if (sms.message == "STOP") {
      db.clientOptedIn.remove(sms.sender).flatMap { _ =>
        sendSMSToTopic(sms, "sms-output")
        val ackSMS = SMS(sender = optInNumber, recipient = sms.sender, message = "Usługa filtrowania phishingu wyłączona")
        sendSMSToTopic(ackSMS, "sms-output")
      }
    } else {
      sendSMSToTopic(sms, "sms-output")
    }
  }

  def handleRegularSMS(sms: SMS): Future[RecordMetadata] = {
    val acceptSMSF = db.clientOptedIn.exists(sms.sender).flatMap {
      case true => smsChecker.checkIfSMSIsSafe(sms)
      case false => Future.successful(true)
    }
    acceptSMSF.flatMap { acceptSMS =>
      val topic = if (acceptSMS) {
        "sms-output"
      } else {
        "sms-rejected"
      }
      sendSMSToTopic(sms, topic)
    }
  }

  def sendSMSToTopic(sms: SMS, topic: String): Future[RecordMetadata] = {
    val promise = Promise[RecordMetadata]()
    println(s"sending $sms to $topic")
    val outputRecord = new ProducerRecord[String, String]("sms-output", sms.asJson.noSpaces)
    producer.send(outputRecord, (metadata, exception) => {
      if (exception != null) {
        promise.failure(exception)
      } else {
        promise.success(metadata)
      }
    })
    promise.future
  }

  def main(args: Array[String]): Unit = {
    try {
      while (true) {
        val records = consumer.poll(Duration.ofMillis(100))
        records.forEach { record =>
          decode[SMS](record.value()) match {
            case Left(error) => println(s"Error: $error")
            case Right(sms) =>
              if (sms.recipient == optInNumber) {
                handleOptIn(sms)
              } else {
                handleRegularSMS(sms)
              }
          }
        }
      }
    } catch {
      case e: InterruptedException => println("Shutting down...")
    } finally {
      consumer.close()
      producer.close()
    }
  }
}
