package pkg.db

import com.outworkers.phantom.dsl.{CassandraConnection, Database}

class PhilterCassandraDB(override val connector: CassandraConnection) extends Database[PhilterCassandraDB](connector) with PhilterDB {
  object clientOptedIn extends ClientOptedInTable with Connector
  object urlIsSafe extends UrlIsSafeTable with Connector
}
