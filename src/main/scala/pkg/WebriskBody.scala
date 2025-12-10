package pkg

import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

case class WebriskBody(uri: String, threatTypes: List[String] = List("SOCIAL_ENGINEERING"), allowScan: Boolean = true)
object WebriskBody {
  implicit val encoder: Encoder[WebriskBody] = deriveEncoder[WebriskBody]
}

