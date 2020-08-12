name := "DataChallenge"

lazy val commonSettings = Seq(
    version := "0.1-SNAPSHOT",
    organization := "com.paypay",
    scalaVersion := "2.12.11"
)

lazy val app = (project in file(".")).
  settings(commonSettings: _*).
  settings(
      libraryDependencies ++= Seq(
          "org.apache.spark" % "spark-core_2.12" % "2.4.6",
          "org.apache.spark" % "spark-sql_2.12" % "2.4.6"
      )
  )