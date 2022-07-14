import Dependencies._

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.16"

lazy val baseSettings: Seq[Setting[_]] = Seq(
  scalacOptions ++= Seq(
    "-feature",
    "-deprecation",
    "-language:implicitConversions",
    "-language:higherKinds",
    "-encoding",
    "UTF-8",
    "-language:existentials",
    "-language:postfixOps",
    "-unchecked",
    "-Ywarn-value-discard"
  ),
  addCompilerPlugin(
    "org.typelevel" %% "kind-projector" % "0.13.2" cross CrossVersion.full
  ),
  libraryDependencies ++= deps
)

lazy val root = (project in file("."))
  .settings(name := "functional-spark")
  .settings(baseSettings: _*)

lazy val common = project
  .settings(baseSettings: _*)

lazy val functional = project
  .settings(baseSettings: _*)
  .dependsOn(common)

Compile / run := Defaults
  .runTask(
    Compile / fullClasspath,
    Compile / run / mainClass,
    Compile / run / runner
  )
  .evaluated
