import scala.xml.transform.{RewriteRule, RuleTransformer}
import Dependencies._

ThisBuild / scalaVersion := "2.13.4"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / organization := "org.sciscala"
ThisBuild / organizationName := "sciscala"

val commonDeps = csvParser ++ scalaTest ++ spire ++ simulacrum ++ parser

lazy val root = (project in file("."))
  .aggregate(common, core, arrow)
//.settings(settings)

lazy val common = project.settings(
  name := "common",
  resolvers ++= Seq(
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
    Resolver.bintrayRepo("zamblauskas", "maven")
  ),
  libraryDependencies ++= commonDeps
)

lazy val core = (project in file("core"))
  .settings(
    name := "simpledf-core",
    scalacOptions += "-Ymacro-annotations",
    libraryDependencies ++= commonDeps
  )
  .dependsOn(common)

lazy val arrow = (project in file("arrow"))
  .dependsOn(core)
  .settings(
    name := "simpledf-arrow",
    scalacOptions += "-Ymacro-annotations",
    libraryDependencies ++= ("org.apache.arrow" % "arrow-vector" % "0.17.1" +: commonDeps)
  )

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-unchecked",
  "-language:postfixOps",
  "-language:higherKinds",
  "-Ymacro-annotations",
  //"-Ywarn-unused-import",
  "-Wvalue-discard",
)
