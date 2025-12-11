package pkg

import com.outworkers.phantom.dsl._

import scala.concurrent.Future

abstract class UrlIsSafeTable extends Table[UrlIsSafeTable, UrlIsSafe] {
  override def tableName: String = "url_is_safe"

  private val ttl = sys.env.getOrElse("URL_CACHE_TTL_SECONDS", "86400").toInt

  object url extends StringColumn with PartitionKey
  object isSafe extends BooleanColumn

  def get(url: String): Future[Option[Boolean]] =
    select(_.isSafe)
      .where(_.url eqs url)
      .one()

  def add(url: String, isSafe: Boolean): Future[ResultSet] =
    insert
      .value(_.url, url)
      .value(_.isSafe, isSafe)
      .ttl(ttl)
      .future()
}
