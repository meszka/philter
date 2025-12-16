import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock
import pkg._

import scala.concurrent.Future

class SMSHandlerTest extends AsyncFlatSpec with Matchers {
  val myDBMock: MyDB = new MyDB {
    override val clientOptedIn: ClientOptedInTable = mock[ClientOptedInTable]
    override val urlIsSafe: UrlIsSafeTable = mock[UrlIsSafeTable]
  }

  when(myDBMock.clientOptedIn.exists("222")).thenReturn(Future.successful(true))
  when(myDBMock.clientOptedIn.exists("333")).thenReturn(Future.successful(false))
  when(myDBMock.clientOptedIn.add(any[String])).thenReturn(Future.successful(()))
  when(myDBMock.clientOptedIn.remove(any[String])).thenReturn(Future.successful(()))

  val smsCheckerMock = mock[SMSChecker]
  val safeSMS = SMS("111", "222", "Zwykły SMS")
  val dangerousSMS = SMS("111", "222", "Kliknij tu: https://www.m-bonk.pl/straszne/rzeczy")
  when(smsCheckerMock.checkIfSMSIsSafe(safeSMS)).thenReturn(Future.successful(true))
  when(smsCheckerMock.checkIfSMSIsSafe(dangerousSMS)).thenReturn(Future.successful(false))

  val smsProducerMock = mock[SMSProducer]
  when(smsProducerMock.sendSMSToTopic(any[SMS], any[String])).thenReturn(Future.successful(()))

  val optInNumber = "123"
  val smsHandler = new SMSHandler(myDBMock, smsProducerMock, smsCheckerMock, optInNumber)

  "handle" should "for an opted-in recipient, send a safe sms to sms-output" in {
    val future = smsHandler.handle(safeSMS)
    future.map { _ =>
      verify(smsProducerMock).sendSMSToTopic(safeSMS, "sms-output")
      succeed
    }
  }

  it should "for an opted-in recipient, send a dangerous sms to sms-rejected" in {
    val future = smsHandler.handle(dangerousSMS)
    future.map { _ =>
      verify(smsProducerMock).sendSMSToTopic(dangerousSMS, "sms-rejected")
      succeed
    }
  }

  it should "for an opted-out recipient, send any sms to sms-output" in {
    val anySMS = SMS("111", "333", "Cokolwiek")
    val future = smsHandler.handle(anySMS)
    future.map { _ =>
      verify(smsProducerMock).sendSMSToTopic(anySMS, "sms-output")
      succeed
    }
  }

  it should "save opt-in status and send a confirmation for a START sms sent to the opt-in number" in {
    val startSMS = SMS("444", optInNumber, "START")
    val future = smsHandler.handle(startSMS)
    future.map { _ =>
      verify(myDBMock.clientOptedIn).add("444")
      verify(smsProducerMock).sendSMSToTopic(startSMS, "sms-output")
      verify(smsProducerMock).sendSMSToTopic(SMS(optInNumber, "444", smsHandler.optInConfirmationMessage), "sms-output")
      succeed
    }
  }

  it should "save opt-in status and send a confirmation for a STOP sms sent to the opt-in number" in {
    val stopSMS = SMS("444", optInNumber, "STOP")
    val future = smsHandler.handle(stopSMS)
    future.map { _ =>
      verify(myDBMock.clientOptedIn).remove("444")
      verify(smsProducerMock).sendSMSToTopic(stopSMS, "sms-output")
      verify(smsProducerMock).sendSMSToTopic(SMS(optInNumber, "444", smsHandler.optOutConfirmationMessage), "sms-output")
      succeed
    }
  }
}
