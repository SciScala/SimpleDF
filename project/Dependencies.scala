import sbt._

object Dependencies {

  lazy val simulacrum = Seq(
    "org.typelevel" %% "simulacrum" % "1.0.0"
    //"org.typelevel" % "simulacrum-scalafix-annotations_2.13" % "0.2.0"
  )
  lazy val spire = Seq("org.typelevel" %% "spire" % "0.17.0-M1")
  lazy val scalaTest = Seq("org.scalatest" %% "scalatest" % "3.1.1")
  lazy val csvParser = Seq(
    "com.opencsv" %  "opencsv"       % "5.3",
    "zamblauskas" % "scala-csv-parser_2.12" % "0.11.6"
  )
}
