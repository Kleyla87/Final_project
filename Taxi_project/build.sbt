version in ThisBuild := "0.1.0-SNAPSHOT"

scalaVersion in ThisBuild := "2.13.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.3.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.3.1"

lazy val root = (project in file("."))
  .settings(
    name := "Taxi_project"
  )
