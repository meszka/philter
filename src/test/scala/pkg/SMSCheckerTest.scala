package pkg

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock
import pkg.db.{ClientOptedInTable, PhilterDB, UrlIsSafeTable}

import scala.concurrent.Future

class SMSCheckerTest extends AsyncFlatSpec with Matchers {
  val dbMock: PhilterDB = new PhilterDB {
    override val clientOptedIn: ClientOptedInTable = mock[ClientOptedInTable]
    override val urlIsSafe: UrlIsSafeTable = mock[UrlIsSafeTable]
  }
  when(dbMock.urlIsSafe.get("https://www.m-bonk.pl/straszne/rzeczy")).thenReturn(Future.successful(None))
  when(dbMock.urlIsSafe.get("https://en.wikipedia.org")).thenReturn(Future.successful(None))
  when(dbMock.urlIsSafe.get("https://duckduckgo.com")).thenReturn(Future.successful(Some(true)))
  when(dbMock.urlIsSafe.get("https://www.m-bonk.pl/znane/rzeczy")).thenReturn(Future.successful(Some(false)))
  when(dbMock.urlIsSafe.add(any[String], any[Boolean])).thenReturn(Future.successful(()))

  val linkCheckerMock: LinkChecker = mock[LinkChecker]
  when(linkCheckerMock.checkIfUrlIsSafe("https://www.m-bonk.pl/straszne/rzeczy")).thenReturn(Future.successful(Some(false)))
  when(linkCheckerMock.checkIfUrlIsSafe("https://en.wikipedia.org")).thenReturn(Future.successful(Some(true)))

  val smsChecker = new SMSChecker(dbMock, linkCheckerMock)

  "checkIfSMSIsSafe" should "return true for an SMS without a link" in {
    val sms = SMS("111", "222", "Wiadomość bez linku")
    smsChecker.checkIfSMSIsSafe(sms).map { _ should be (true) }
  }

  it should "return false for an SMS with a dangerous link and add it to the cache" in {
    val sms = SMS("111", "222", "Kliknij tu: https://www.m-bonk.pl/straszne/rzeczy")
    val resultF = smsChecker.checkIfSMSIsSafe(sms)
    resultF.map { result =>
      verify(linkCheckerMock).checkIfUrlIsSafe("https://www.m-bonk.pl/straszne/rzeczy")
      verify(dbMock.urlIsSafe).add("https://www.m-bonk.pl/straszne/rzeczy", false)
      result should be (false)
    }
  }

  it should "return true for an SMS with a safe link and add it to the cache" in {
    val sms = SMS("111", "222", "Zobacz jaka super stronka! https://en.wikipedia.org")
    val resultF = smsChecker.checkIfSMSIsSafe(sms)
    resultF.map { result =>
      verify(linkCheckerMock).checkIfUrlIsSafe("https://en.wikipedia.org")
      verify(dbMock.urlIsSafe).add("https://en.wikipedia.org", true)
      result should be (true)
    }
  }

  it should "return true without checking the link and refresh the cache for an SMS with a safe link already in the cache" in {
    val sms = SMS("111", "222", "Zobacz jaka super stronka! https://duckduckgo.com")
    val resultF = smsChecker.checkIfSMSIsSafe(sms)
    resultF.map { result =>
      verify(linkCheckerMock, times(0)).checkIfUrlIsSafe("https://duckduckgo.com")
      verify(dbMock.urlIsSafe).add("https://duckduckgo.com", true)
      result should be (true)
    }
  }

  it should "return false without checking the link and refresh the cache for an SMS with a dangerous link already in the cache" in {
    val sms = SMS("111", "222", "Kliknij tu: https://www.m-bonk.pl/znane/rzeczy")
    val resultF = smsChecker.checkIfSMSIsSafe(sms)
    resultF.map { result =>
      verify(linkCheckerMock, times(0)).checkIfUrlIsSafe("https://www.m-bonk.pl/znane/rzeczy")
      verify(dbMock.urlIsSafe).add("https://www.m-bonk.pl/znane/rzeczy", false)
      result should be (false)
    }
  }
}
