val buildSettings = Seq(
  organization := "net.janvsmachine",
  name := "spark-sessions",
  version := "0.0.1",
  scalaVersion := "2.11.8",
  scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8", "-target:jvm-1.8", "-Xfatal-warnings", "-Xfuture")
)

val sparkVersion = "2.1.1"
val awsVersion = "1.11.61"

val dependencySettings = Seq(
  libraryDependencies ++= {
    Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "com.amazonaws" % "aws-java-sdk-core" % awsVersion % Provided,
      "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0",
      "org.scalatest" %% "scalatest" % "3.0.1" % Test
    )
  }
)

val root = (project in file(".")).
  settings(buildSettings: _*).
  settings(dependencySettings: _*)
