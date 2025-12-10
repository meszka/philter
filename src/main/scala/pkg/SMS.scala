package pkg

import io.circe.{Decoder, Encoder}
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}


case class SMS(sender: String, recipient: String, message: String)

object SMS {
  implicit val decoder: Decoder[SMS] = deriveDecoder[SMS]
  implicit val encoder: Encoder[SMS] = deriveEncoder[SMS]
}
