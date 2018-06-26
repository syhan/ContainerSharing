name := "seawide-container-extractor"

version := "0.1"

scalaVersion := "2.11.8"

developers := List(
  Developer("syhan", "Song Yihan", "@syhan", url("https://github.com/syhan"))
)
organization := "com.sap"
organizationName := "seawide"
organizationHomepage := Option(url("http://seawider.com"))
scmInfo := Option(ScmInfo(url("https://github.com/syhan/seawide")))

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.3.1"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka-0-10_2.11" % "2.3.1"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.1.0"
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "1.1.0"

libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.0"
libraryDependencies += "org.elasticsearch" % "elasticsearch-hadoop" % "6.2.1"
libraryDependencies += "com.typesafe.play" %%  "play-json" % "2.4.11"


