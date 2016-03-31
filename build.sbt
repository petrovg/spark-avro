name := "spark-avro"
organization := "spark-avro"
version := "1.0.0"
scalaVersion := "2.11.8"

resolvers in ThisBuild += "apache-snapshots" at "http://repository.apache.org/snapshots/"

sparkVersion := "2.0.0-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0-SNAPSHOT",
  "org.apache.spark" %% "spark-sql" % "2.0.0-SNAPSHOT",
  "org.scalatest" %% "scalatest" % "2.2.1" % Test
)


licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))