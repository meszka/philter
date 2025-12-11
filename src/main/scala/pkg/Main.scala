import com.outworkers.phantom.dsl.{CreateQueryOps, context}

import java.time.Duration
import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.nibor.autolink.{LinkExtractor, LinkType}
import sttp.client4.{Backend, DefaultFutureBackend, Response, UriContext, quickRequest}

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.jdk.CollectionConverters.SetHasAsJava
import scala.jdk.CollectionConverters.IterableHasAsScala
import io.circe.parser.decode
import io.circe.syntax.EncoderOps
import pkg.{MyDBProvider, SMS, WebriskBody, WebriskResponse}

object Main extends MyDBProvider {
  val linkExtractor: LinkExtractor = LinkExtractor.builder().linkTypes(Set(LinkType.URL).asJava).build()
  val sttpBackend: Backend[Future] = DefaultFutureBackend()
  val googleApiKey: String = sys.env.getOrElse("GOOGLE_API_KEY", "fake")
  val optInNumber = "123"

  Await.result(
    db.clientOptedIn.create.ifNotExists().future(),
    10.seconds
  )

  def checkIfSMSIsSafe(sms: SMS): Future[Boolean] = {
    println("checking if sms is safe")
    val linkSpans = linkExtractor.extractLinks(sms.message).asScala
    val links = linkSpans.map(linkSpan => sms.message.substring(linkSpan.getBeginIndex, linkSpan.getEndIndex))
    val checks = Future.sequence(links.map(checkIfURLIsSafe))
    checks.map(_.forall(isSafeOpt => isSafeOpt.getOrElse(true)))
  }

  def checkIfURLIsSafe(url: String): Future[Option[Boolean]] = {
    val isSafeOptF = db.urlIsSafe.get(url).flatMap {
      case Some(isSafe) => Future.successful(Some(isSafe))
      case None =>
        if (googleApiKey == "fake") {
          checkIfURLIsSafeReal(url)
        } else {
          checkIfURLIsSafeFake(url)
        }
    }
    isSafeOptF.foreach(_.foreach(isSafe => db.urlIsSafe.add(url, isSafe)))
    isSafeOptF
  }

  def checkIfURLIsSafeReal(url: String): Future[Option[Boolean]] = {
    val body = WebriskBody(uri=url).asJson.noSpaces
    val response: Future[Response[String]] = quickRequest
      .body(body)
      .post(uri"https://webrisk.googleapis.com/v1eap1:evaluateUri?key=$googleApiKey")
      .send(sttpBackend)
    response.map { response =>
      decode[WebriskResponse](response.body).map { response =>
        response.scores.exists(score => score.confidenceLevel == "EXTREMELY_HIGH")
      }.toOption
    }
    // TODO: retry on failure?
  }

  def checkIfURLIsSafeFake(url: String): Future[Option[Boolean]] = {
    Future {
      Thread.sleep(1000)
      Option(!url.contains("m-bonk"))
    }
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
              if (sms.recipient == optInNumber && sms.message == "START") {
                db.clientOptedIn.add(sms.sender).map { _ =>
                  val outputRecord = new ProducerRecord[String, String]("sms-output", record.key(), record.value())
                  println(s"sending output record to sms-output")
                  producer.send(outputRecord)
                }
              } else if (sms.recipient == optInNumber && sms.message == "STOP") {
                db.clientOptedIn.remove(sms.sender).map { _ =>
                  val outputRecord = new ProducerRecord[String, String]("sms-output", record.key(), record.value())
                  println(s"sending output record to sms-output")
                  producer.send(outputRecord)
                }
              } else {
                val acceptSMSF = db.clientOptedIn.get(sms.sender).map(_.isDefined).flatMap {
                  case true => checkIfSMSIsSafe(sms)
                  case false => Future.successful(true)
                }
                acceptSMSF.map { acceptSMS =>
                  val topic = if (acceptSMS) {
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
