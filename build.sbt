name := "VTS_Dashboard"
organization := "viettel.vts"
version := "0.1"
scalaVersion := "2.11.11"
autoScalaLibrary := false

unmanagedBase := baseDirectory.value / "lib"

val sparkVersion = "2.4.0"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.hive" % "hive-jdbc" % "2.0.0",
  "mysql" % "mysql-connector-java" % "5.1.12"
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

libraryDependencies ++= sparkDependencies ++ testDependencies
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion