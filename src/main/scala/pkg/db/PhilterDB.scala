package pkg.db

trait PhilterDB {
  val clientOptedIn: ClientOptedInTable
  val urlIsSafe: UrlIsSafeTable
}
