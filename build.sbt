import sbt.Keys.libraryDependencies

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "philter"
  )

val circeVersion = "0.14.15"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "4.1.1",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "org.nibor.autolink" % "autolink" % "0.12.0",
  "com.softwaremill.sttp.client4" %% "core" % "4.0.13",
  "com.outworkers" %% "phantom-dsl" % "2.59.0",
  "com.outworkers" %% "phantom-connectors" % "2.59.0",
  "org.scala-lang" % "scala-reflect" % scalaVersion.value,
  "org.scalactic" %% "scalactic" % "3.2.19",
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "org.scalatestplus" %% "mockito-5-10" % "3.2.18.0" % Test,
)

Test / parallelExecution := false

assembly / assemblyMergeStrategy := {
  case "module-info.class" =>
    MergeStrategy.discard
  case PathList("META-INF", "io.netty.versions.properties") =>
    MergeStrategy.first
  case PathList("META-INF", xs@_*) =>
    xs.map(_.toLowerCase) match {
      case "services" :: xs => MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.discard
    }
  case other =>
    (assembly / assemblyMergeStrategy).value(other)
}
