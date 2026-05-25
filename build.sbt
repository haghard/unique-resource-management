ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.13.18"

val releaseJvmVersion = "17"

initialize := {
  val _ = initialize.value
  val current  = sys.props("java.specification.version")
  if (current != releaseJvmVersion)
    sys.error(s"Java $releaseJvmVersion is required for this project. Found $current instead.")
}
//export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home
javaHome := Some(file("/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home/"))

/*
Akka-2.7.0(22.10)
https://doc.akka.io/reference/release-notes/
*/

val AkkaVersion = "2.7.0"
val AkkaHttpVersion = "10.4.0"
val AkkaManagementVersion = "1.2.0"
val AkkaPersistenceJdbcV = "5.2.0"
val AkkaPersistenceR2dbcVersion = "1.0.1"
val AkkaProjectionVersion = sys.props.getOrElse("akka-projection.version", "1.3.0")

lazy val java17Settings = Seq(
  "--add-opens",
  "java.base/java.nio=ALL-UNNAMED",
  "--add-opens",
  "java.base/sun.nio.ch=ALL-UNNAMED"
)

lazy val root = (project in file("."))
  .settings(
    name := "unique-resource-management",
    javaOptions ++= java17Settings,
  )
  .enablePlugins(AkkaGrpcPlugin)

//https://mvnrepository.com/artifact/com.lihaoyi/ammonite
//https://github.com/com-lihaoyi/Ammonite/releases
val AmmoniteVersion = "3.0.9"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-slf4j"  % AkkaVersion,
  "com.typesafe.akka" %% "akka-persistence-typed"       % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed"  % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools"           % AkkaVersion,

  "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
  //"com.github.jaceksokol" %% "akka-stream-map-async-partition" % "1.0.3",

  "com.typesafe.akka"             %% "akka-discovery"               % AkkaVersion,
  "com.lightbend.akka.management" %% "akka-management-cluster-bootstrap" % AkkaManagementVersion,
  "com.lightbend.akka.management" %% "akka-management-cluster-http" % AkkaManagementVersion,

  "ch.qos.logback" % "logback-classic" % "1.5.32",
  "org.slf4j"      % "slf4j-api"       %  "2.0.18",

  "io.aeron" % "aeron-driver" % "1.46.9", //1.47.0
  "io.aeron" % "aeron-client" % "1.46.9",

  "org.wvlet.airframe" %% "airframe-ulid" % "2026.1.6",

  "com.lightbend.akka" %% "akka-persistence-r2dbc" % AkkaPersistenceR2dbcVersion,

  //"com.lightbend.akka" %% "akka-projection-durable-state" % "1.4.0",
  "com.lightbend.akka" %% "akka-projection-r2dbc" %  AkkaPersistenceR2dbcVersion,
  "com.lightbend.akka" %% "akka-projection-eventsourced" % AkkaProjectionVersion,

  //"org.hdrhistogram" % "HdrHistogram" % "2.2.2",
  "com.lihaoyi" % "ammonite" % AmmoniteVersion % "test" cross CrossVersion.full
)

addCommandAlias("c", "compile")
addCommandAlias("r", "reload")

enablePlugins(JavaAppPackaging, DockerPlugin)
dockerBaseImage := "docker.io/library/adoptopenjdk:17-jre-hotspot"
dockerUsername := sys.props.get("docker.username")
dockerRepository := sys.props.get("docker.registry")
dockerUpdateLatest := true
ThisBuild / dynverSeparator := "-"

Compile / scalacOptions ++= Seq(
  "-Xsource:3",
  //"-Xsource:3-cross",
  "-Wconf:msg=lambda-parens:s",
  s"-release:$releaseJvmVersion", // tells the Scala compiler to emit bytecode that is compatible with JDK N.
  "-Xlog-reflective-calls",
  "-Xlint",
  "-Vtype-diffs",
  "-Xmigration", //Emit migration warnings under -Xsource:3 as fatal warnings, not errors; -Xmigration disables fatality (#10439 by @som-snytt, #10511)
  "-Vimplicits", // makes the compiler print implicit resolution chains when no implicit value can be found
  "-Ylog-classpath", //log classpath
  s"-Wconf:src=${(Compile / target).value}/scala-2.13/akka-grpc/.*:silent",
  "-Wconf:msg=Marked as deprecated in proto file:silent",
  //"-Wconf:cat=other-match-analysis:error", //Make only some warnings fatal: Transform exhaustivity warnings into errors.
  "-Xfatal-warnings", // Fail the compilation if there are any warnings.
)

javacOptions ++= Seq("-source", releaseJvmVersion, "-target", releaseJvmVersion)

scalafmtOnCompile := true

run / fork := false
//run / fork := true

// Allow ctrl-c to kill forked tasks without killing SBT
//Global / cancelable := true

dependencyOverrides ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed"             % AkkaVersion,
  "com.typesafe.akka" %% "akka-protobuf"                % AkkaVersion,
  "com.typesafe.akka" %% "akka-protobuf-v3"             % AkkaVersion,
  "com.typesafe.akka" %% "akka-persistence"             % AkkaVersion,
  "com.typesafe.akka" %% "akka-actor"                   % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster"                 % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster-sharding-typed"  % AkkaVersion,
  "com.typesafe.akka" %% "akka-coordination"            % AkkaVersion,
  "com.typesafe.akka" %% "akka-stream"                  % AkkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools"           % AkkaVersion,
)

//test:run

Test / sourceGenerators += Def.task {
  val file = (Test / sourceManaged).value / "amm.scala"
  IO.write(file, """object amm extends App { ammonite.Main().run() }""")
  Seq(file)
}.taskValue

