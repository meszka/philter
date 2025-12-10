import java.time.Duration
import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.nibor.autolink.{LinkExtractor, LinkType}

import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.CollectionConverters.SetHasAsJava
import scala.jdk.CollectionConverters.IterableHasAsScala
import io.circe.parser.decode
import pkg.SMS

object Main {
  val linkExtractor: LinkExtractor = LinkExtractor.builder().linkTypes(Set(LinkType.URL).asJava).build()

  def checkIfSMSIsSafe(sms: SMS): Boolean = {
    val linkSpans = linkExtractor.extractLinks(sms.message).asScala
    val links = linkSpans.map(linkSpan => sms.message.substring(linkSpan.getBeginIndex, linkSpan.getEndIndex))
    links.forall(checkIfURLIsSafe)
  }

  def checkIfURLIsSafe(url: String): Boolean = {
    !url.contains("m-bonk")
  }

  def main(args: Array[String]): Unit = {
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

    try {
      while (true) {
        val records = consumer.poll(Duration.ofMillis(100))
        records.forEach { record =>
          decode[SMS](record.value()) match {
            case Left(error) => println(s"Error: $error")
            case Right(sms) =>
              val smsIsSafe = checkIfSMSIsSafe(sms)
              val topic = if (smsIsSafe) {
                "sms-output"
              } else {
                "sms-rejected"
              }
              val outputRecord = new ProducerRecord[String, String](topic, record.key(), record.value())
              println(s"sending output record to $topic")
              producer.send(outputRecord)
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
