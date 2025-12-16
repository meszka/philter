package pkg.db

import com.outworkers.phantom.dsl.{CassandraConnection, Database}

class MyCassandraDB(override val connector: CassandraConnection) extends Database[MyCassandraDB](connector) with MyDB {
  object clientOptedIn extends ClientOptedInTable with Connector
  object urlIsSafe extends UrlIsSafeTable with Connector
}
