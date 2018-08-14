name := "akkaRoutingExample"

version := "0.1"

scalaVersion := "2.12.6"

libraryDependencies ++= {
  val akkaVersion = "2.5.14"
  Seq(
    "com.typesafe.akka" %% "akka-actor"   % akkaVersion,
    "com.typesafe.akka" %% "akka-remote"  % akkaVersion,
    "com.typesafe.akka" %% "akka-slf4j"   % akkaVersion,
    "com.typesafe.akka" %% "akka-testkit" % akkaVersion  % "test",
    "org.scalatest"     %% "scalatest"    % "3.0.0"      % "test"
  )
}