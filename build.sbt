name := "ZIO Tests"

version := "0.1"

scalaVersion := "2.12.8"

fork in run := true

val zioVersion = "1.0.0-RC10-1"
val circeVersion = "0.11.1"

// better type inference when multiple type parameters are involved and they need to be inferred in multiple steps
// i.e. conversion from Function1[Int, Int] to (approximately) Function1[Int][Int]
scalacOptions ++= Seq(
  "-feature",
  "-Ypartial-unification",
  "-Ywarn-value-discard"
)

//javaOptions += "-agentpath:/usr/share/visualvm/profiler/lib/deployed/jdk16/linux-amd64/libprofilerinterface.so=/usr/share/visualvm/profiler/lib,5140"

resolvers += Resolver.bintrayRepo("cakesolutions", "maven")
resolvers += Resolver.sonatypeRepo("public")

libraryDependencies ++= Seq(
  "org.scalaz" %% "scalaz-core" % "7.2.27",
  "dev.zio" %% "zio" % zioVersion,
  "dev.zio" %% "zio-streams" % zioVersion,
  "dev.zio" %% "zio-metrics" % "0.0.2",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "io.circe" %% "circe-optics" % "0.11.0",
  "net.cakesolutions" %% "scala-kafka-client" % "2.1.0",
  "io.prometheus" % "simpleclient" % "0.6.0",
  "io.prometheus" % "simpleclient_httpserver" % "0.6.0"
)

// https://www.scala-sbt.org/1.x/docs/Community-Plugins.html

// http://www.wartremover.org/
//wartremoverErrors ++= Warts.unsafe
