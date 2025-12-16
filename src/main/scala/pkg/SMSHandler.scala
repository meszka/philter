package pkg

import pkg.db.PhilterDB

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class SMSHandler(db: PhilterDB, smsProducer: SMSProducer, smsChecker: SMSChecker, optInNumber: String) {
  val optInConfirmationMessage = "Usługa filtrowania phishingu włączona"
  val optOutConfirmationMessage = "Usługa filtrowania phishingu wyłączona"

  def handle(sms: SMS): Future[Unit] = {
    if (sms.recipient == optInNumber) {
      handleOptIn(sms)
    } else {
      handleRegularSMS(sms)
    }
  }

  private def handleOptIn(sms: SMS): Future[Unit] = {
    if (sms.message == "START") {
      val ackSMS = SMS(sender = optInNumber, recipient = sms.sender, message = optInConfirmationMessage)
      for {
        _ <- db.clientOptedIn.add(sms.sender)
        _ <- smsProducer.sendSMSToTopic(sms, "sms-output")
        _ <- smsProducer.sendSMSToTopic(ackSMS, "sms-output")
      } yield ()
    } else if (sms.message == "STOP") {
      val ackSMS = SMS(sender = optInNumber, recipient = sms.sender, message = optOutConfirmationMessage)
      for {
        _ <- db.clientOptedIn.remove(sms.sender)
        _ <- smsProducer.sendSMSToTopic(sms, "sms-output")
        _ <- smsProducer.sendSMSToTopic(ackSMS, "sms-output")
      } yield ()
    } else {
      smsProducer.sendSMSToTopic(sms, "sms-output")
    }
  }

  private def handleRegularSMS(sms: SMS): Future[Unit] = {
    val acceptSMSF = db.clientOptedIn.exists(sms.recipient).flatMap {
      case true => smsChecker.checkIfSMSIsSafe(sms)
      case false => Future.successful(true)
    }
    acceptSMSF.flatMap { acceptSMS =>
      val topic = if (acceptSMS) {
        "sms-output"
      } else {
        "sms-rejected"
      }
      smsProducer.sendSMSToTopic(sms, topic)
    }
  }
}
