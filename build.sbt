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
