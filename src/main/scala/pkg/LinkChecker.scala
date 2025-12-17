package pkg

import io.circe.parser.decode
import io.circe.syntax.EncoderOps
import pkg.webrisk.{WebriskBody, WebriskResponse}
import sttp.client4.{Backend, DefaultFutureBackend, Response, UriContext, quickRequest}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class LinkChecker(googleApiKey: String) {
  val sttpBackend: Backend[Future] = DefaultFutureBackend()

  def checkIfUrlIsSafe(url: String): Future[Option[Boolean]] = {
    if (googleApiKey == "fake") {
      checkIfURLIsSafeFake(url)
    } else {
      checkIfURLIsSafeReal(url)
    }
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
  }

  def checkIfURLIsSafeFake(url: String): Future[Option[Boolean]] = {
    Future {
      Thread.sleep(1000)
      Option(!url.contains("m-bonk"))
    }
  }
}
