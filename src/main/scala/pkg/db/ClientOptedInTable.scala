package pkg.db

import com.outworkers.phantom.dsl._

import scala.concurrent.Future

abstract class ClientOptedInTable extends Table[ClientOptedInTable, ClientOptedIn] {
  override def tableName: String = "client_opted_in"

  object client extends StringColumn with PartitionKey

  def exists(clientId: String): Future[Boolean] =
    select
      .where(_.client eqs clientId)
      .one()
      .map(_.isDefined)

  def add(clientId: String): Future[Unit] =
    insert
      .value(_.client, clientId)
      .consistencyLevel_=(ConsistencyLevel.QUORUM)
      .future()
      .map(_ => ())

  def remove(clientId: String): Future[Unit] =
    delete
      .where(_.client eqs clientId)
      .consistencyLevel_=(ConsistencyLevel.QUORUM)
      .future()
      .map(_ => ())
}
