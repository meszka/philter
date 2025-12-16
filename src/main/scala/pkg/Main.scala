package pkg

import com.outworkers.phantom.dsl.{CreateQueryOps, context}

import java.time.Duration
import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters.{IterableHasAsScala, SeqHasAsJava}
import io.circe.parser.decode
import pkg.db.PhilterDBProvider

object Main extends App with PhilterDBProvider {
  val googleApiKey: String = sys.env.getOrElse("GOOGLE_API_KEY", "fake")
  val optInNumber: String = sys.env.getOrElse("OPT_IN_NUMBER", "123")
  val bootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

  val consumerProps = new Properties()
  consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "philter-consumer-group")
  consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
  consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val producerProps = new Properties()
  producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
  producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  val linkChecker = new LinkChecker(googleApiKey)
  val smsChecker = new SMSChecker(db, linkChecker)
  val consumer = new KafkaConsumer[String, String](consumerProps)
  val producer = new KafkaProducer[String, String](producerProps)
  val smsProducer = new SMSProducer(producer)
  val smsHandler = new SMSHandler(db, smsProducer, smsChecker, optInNumber)

  println("Waiting for Cassandra...")
  Thread.sleep(30000)
  println("Done waiting")
  Await.result(db.clientOptedIn.create.ifNotExists().future(), 10.seconds)
  Await.result(db.urlIsSafe.create.ifNotExists().future(), 10.seconds)

  consumer.subscribe(List("sms-input").asJava)

  val timeout = scala.concurrent.duration.Duration(10, scala.concurrent.duration.MINUTES)

  try {
    while (true) {
      val records = consumer.poll(Duration.ofMillis(100))
      val futures = records.asScala.map { record =>
        decode[SMS](record.value()) match {
          case Left(error) =>
            println(s"Error decoding SMS: $error")
            Future(())
          case Right(sms) =>
            smsHandler.handle(sms)
        }
      }
      Await.result(Future.sequence(futures), timeout)
    }
  } catch {
    case _: InterruptedException => println("Shutting down...")
  } finally {
    consumer.close()
    producer.close()
  }
}
