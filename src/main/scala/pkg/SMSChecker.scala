package pkg

import org.nibor.autolink.{LinkExtractor, LinkType}
import pkg.db.MyDB

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.jdk.CollectionConverters.{IterableHasAsScala, SetHasAsJava}

class SMSChecker(db: MyDB, linkChecker: LinkChecker) {
  val linkExtractor: LinkExtractor = LinkExtractor.builder().linkTypes(Set(LinkType.URL).asJava).build()

  def checkIfSMSIsSafe(sms: SMS): Future[Boolean] = {
    println("checking if sms is safe")
    val linkSpans = linkExtractor.extractLinks(sms.message).asScala
    val links = linkSpans.map(linkSpan => sms.message.substring(linkSpan.getBeginIndex, linkSpan.getEndIndex))
    val checks = Future.sequence(links.map(checkIfURLIsSafe))
    checks.map(_.forall(isSafeOpt => isSafeOpt.getOrElse(true)))
  }

  private def checkIfURLIsSafe(url: String): Future[Option[Boolean]] = {
    val isSafeOptF = db.urlIsSafe.get(url).flatMap {
      case Some(isSafe) => Future.successful(Some(isSafe))
      case None => linkChecker.checkIfUrlIsSafe(url)
    }
    isSafeOptF.foreach {
      _.foreach(isSafe => {
        println("adding to cache")
        db.urlIsSafe.add(url, isSafe)
      })
    }
    isSafeOptF
  }
}
