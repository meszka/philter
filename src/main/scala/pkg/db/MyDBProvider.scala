package pkg.db

import com.outworkers.phantom.connectors.ContactPoint
import com.outworkers.phantom.dsl.{CassandraConnection, DatabaseProvider}

trait MyDBProvider extends DatabaseProvider[MyCassandraDB] {
  val host = sys.env.getOrElse("CASSANDRA_HOST", "localhost")
  val cassandraConnector: CassandraConnection =
    ContactPoint(host, 9042).noHeartbeat().keySpace("sms_phishing_filter_keyspace")
  val database = new MyCassandraDB(cassandraConnector)
}
