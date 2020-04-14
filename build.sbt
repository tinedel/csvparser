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

homepage := Some(url("https://github.com/tinedel/csvparser"))
scmInfo := Some(ScmInfo(url("https://github.com/tindel/csvparser"),
  "git@github.com:tinedel/csvparser.git"))
developers := List(Developer("tinedel",
  "Ivan Volzhev",
  "ivolzhev@gmail.com",
  url("https://github.com/tinedel")))
licenses += ("MIT", url("https://www.mit.edu/~amini/LICENSE.md"))
publishMavenStyle := true

// Add sonatype repository settings
publishTo := Some(
  if (isSnapshot.value)
    Opts.resolver.sonatypeSnapshots
  else
    Opts.resolver.sonatypeStaging
)

lazy val copyDocAssetsTask = taskKey[Unit]("Copy doc assets")

copyDocAssetsTask := {
  println("Copying doc assets")
  val sourceDir = file("src/main/doc-resources")
  val targetDir = (target in(Compile, doc)).value
  IO.copyDirectory(sourceDir, targetDir)
}

copyDocAssetsTask := copyDocAssetsTask triggeredBy (doc in Compile)