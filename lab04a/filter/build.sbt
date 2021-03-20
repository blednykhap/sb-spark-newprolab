import sbt.Keys._

lazy val commonSettings = Seq(
  version := "1.0",
  name := "filter",
  scalaVersion := "2.11.12"
)

lazy val commonDependencies = Seq(
  "org.apache.spark" %%  "spark-core" % "2.4.7",
  "org.apache.spark" %%  "spark-sql" % "2.4.7"
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

assemblyJarName in assembly := "filter_2.11-1.0.jar"
