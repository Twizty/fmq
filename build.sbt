name := "mmq"

version := "0.1"

scalaVersion := "2.12.6"

val circeVersion = "0.9.0"

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "1.0.0-RC1",
  "org.typelevel" %% "cats-effect" % "0.8",
  "co.fs2" %% "fs2-core" % "0.10.1",
  "co.fs2" %% "fs2-io" % "0.10.1"
)

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser",
  "io.circe" %% "circe-java8",
).map(_ % circeVersion)
