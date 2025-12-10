package pkg

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

case class WebriskResponse(scores: List[WebriskScore])
object WebriskResponse {
  implicit val decoder: Decoder[WebriskResponse] = deriveDecoder[WebriskResponse]
}

