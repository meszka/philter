ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "sms-phishing-filter"
  )

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "4.1.1"
)