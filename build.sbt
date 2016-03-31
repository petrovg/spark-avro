name := "spark-avro"
organization := "spark-avro"
version := "1.0.0"
scalaVersion := "2.11.8"

resolvers in ThisBuild += "apache-snapshots" at "http://repository.apache.org/snapshots/"

sparkVersion := "2.0.0-SNAPSHOT"

libraryDependencies ++= Seq(
  "org.apache.avro" % "avro" % "1.7.6" exclude("org.mortbay.jetty", "servlet-api"),
  "org.apache.avro" % "avro-mapred" % "1.7.7"  % "provided" classifier("hadoop2") exclude("org.mortbay.jetty", "servlet-api"),
  "org.scalatest" %% "scalatest" % "2.2.1" % Test,
  "commons-io" % "commons-io" % "2.4" % Test
)

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % testHadoopVersion.value % Test,
  "org.apache.spark" %% "spark-core" % testSparkVersion.value % Test exclude("org.apache.hadoop", "hadoop-client"),
  "org.apache.spark" %% "spark-sql" % testSparkVersion.value % Test exclude("org.apache.hadoop", "hadoop-client")
)

licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))