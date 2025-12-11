package pkg

import com.outworkers.phantom.dsl.{CassandraConnection, Database}

class MyDB(override val connector: CassandraConnection) extends Database[MyDB](connector) {
  object clientOptedIn extends ClientOptedInTable with Connector
}
