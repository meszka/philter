package pkg.webrisk

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

case class WebriskScore(threatType: String, confidenceLevel: String)
object WebriskScore {
  implicit val decoder: Decoder[WebriskScore] = deriveDecoder[WebriskScore]
}
