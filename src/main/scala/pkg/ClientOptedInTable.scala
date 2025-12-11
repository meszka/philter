package pkg

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

  def add(clientId: String): Future[ResultSet] =
    insert
      .value(_.client, clientId)
      .consistencyLevel_=(ConsistencyLevel.QUORUM)
      .future()

  def remove(clientId: String): Future[ResultSet] =
    delete
      .where(_.client eqs clientId)
      .consistencyLevel_=(ConsistencyLevel.QUORUM)
      .future()
}
