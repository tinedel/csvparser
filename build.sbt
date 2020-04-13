import Dependencies._

ThisBuild / scalaVersion := "2.12.11"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "ua.kyiv.tinedel.csvparser"
ThisBuild / organizationName := "csvparser"

lazy val root = (project in file("."))
  .settings(
    name := "csvparser",
    libraryDependencies += scalaTest % Test
  )

logBuffered in Test := false
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-l", "HugeFileTest")

lazy val HugeFile = config("hugeFile").extend(Test)
configs(HugeFile)
inConfig(HugeFile)(Defaults.testTasks)

testOptions in HugeFile -= Tests.Argument(TestFrameworks.ScalaTest, "-l", "HugeFileTest")
testOptions in HugeFile += Tests.Argument(TestFrameworks.ScalaTest, "-n", "HugeFileTest")