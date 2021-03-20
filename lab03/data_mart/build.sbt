import sbt.Keys._

lazy val commonSettings = Seq(
  version := "1.0",
  name := "data_mart",
  scalaVersion := "2.11.12"
)

lazy val commonDependencies = Seq(
  "org.apache.spark" %%  "spark-core" % "2.4.7",
  "org.apache.spark" %%  "spark-sql" % "2.4.7",
  "org.apache.spark" %%  "spark-mllib" % "2.4.6",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.5.0",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "6.8.9",
  "org.postgresql" % "postgresql" % "42.2.19",
  "org.json4s" %% "json4s-native" % "3.6.11",
  "org.json4s" %% "json4s-jackson" % "3.6.11",
  "org.scala-lang.modules" %% "scala-xml" % "1.2.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    libraryDependencies ++= commonDependencies,
  ).
  enablePlugins(AssemblyPlugin)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := "data_mart_2.11-1.0.jar"

