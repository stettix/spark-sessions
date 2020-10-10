val buildSettings = Seq(
  organization := "net.janvsmachine",
  name := "spark-sessions",
  version := "0.0.2",
  scalaVersion := "2.12.12",
  scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8", "-target:jvm-1.8", "-Xfatal-warnings", "-Xfuture")
)

val sparkVersion = "3.0.0"
val awsVersion = "1.11.828"

val dependencySettings = Seq(
  libraryDependencies ++= {
    Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "com.amazonaws" % "aws-java-sdk-core" % awsVersion % Provided,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
      "org.scalatest" %% "scalatest" % "3.0.5" % Test
    )
  }
)

val assemblySettings = Seq(
  assemblyMergeStrategy in assembly := {
    case PathList("org.apache.spark.sql.sources.DataSourceRegister") => MergeStrategy.concat
    case PathList("META-INF", "services", xs@_) => MergeStrategy.concat
    case PathList("META-INF", "native", xs@_) => MergeStrategy.first
    case PathList("META-INF", xs@_*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
)

val root = (project in file(".")).
  settings(buildSettings: _*).
  settings(dependencySettings: _*).
  settings(assemblySettings: _*)
