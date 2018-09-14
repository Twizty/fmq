name := "mmq"

version := "0.1"

scalaVersion := "2.12.6"

val circeVersion = "0.9.0"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "1.3.0",
  "org.typelevel" %% "cats-effect" % "1.0.0",
  "co.fs2" %% "fs2-core" % "1.0.0-M5",
  "co.fs2" %% "fs2-io" % "1.0.0-M5"
)

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser",
  "io.circe" %% "circe-java8",
).map(_ % circeVersion)
