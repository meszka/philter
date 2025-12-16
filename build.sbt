ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "sms-phishing-filter"
  )

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "4.1.1"
)

val circeVersion = "0.14.15"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

libraryDependencies += "org.nibor.autolink" % "autolink" % "0.12.0"

assembly / assemblyMergeStrategy := {
  case "module-info.class" =>
    MergeStrategy.discard
  case PathList("META-INF", "io.netty.versions.properties") =>
    MergeStrategy.first
  case PathList("META-INF", xs @ _*) =>
    MergeStrategy.discard
  case other =>
    (assembly / assemblyMergeStrategy).value(other)
}

libraryDependencies += "com.softwaremill.sttp.client4" %% "core" % "4.0.13"

libraryDependencies ++= Seq(
  "com.outworkers" %% "phantom-dsl" % "2.59.0",
  "com.outworkers" %% "phantom-connectors" % "2.59.0",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value
)

libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.19"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.19" % Test
libraryDependencies += "org.scalatestplus" %% "mockito-5-10" % "3.2.18.0" % Test