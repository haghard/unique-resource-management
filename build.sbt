
version := "0.2.1"
scalaVersion := "2.13.18"
name := "resources"

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
/*
val AkkaVersion = "2.7.0"
val AkkaHttpVersion = "10.4.0"
val AkkaManagementVersion = "1.2.0"
val AkkaPersistenceJdbcV = "5.2.0"
val AkkaPersistenceR2dbcVersion = "1.0.1"
val AkkaProjectionVersion = sys.props.getOrElse("akka-projection.version", "1.3.0")
*/

/*
Akka-23.5(2.8.2) May 16, 2023
https://doc.akka.io/reference/release-notes/2023-05-16-akka-23.5-released.html

Akka (core) 2.8.2
Akka HTTP 10.5.2
Akka gRPC 2.3.2
Akka Management 1.4.0
Alpakka Kafka 4.0.2
Alpakka 6.0.1
Akka Persistence R2DBC 1.1.0 (+)
Akka Persistence JDBC 5.2.1
Akka Persistence Cassandra 1.1.1
Akka Projections 1.4.0
Akka Diagnostics 2.0.0
*/


val AkkaVersion = "2.8.2"
val AkkaHttpVersion = "10.5.2"
val AkkaManagementVersion = "1.4.0"
val AkkaPersistenceJdbcV = "5.2.1"
val AkkaPersistenceR2dbcVersion = "1.1.0"
val AkkaProjectionVersion = sys.props.getOrElse("akka-projection.version", "1.4.0")

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

  "com.lightbend.akka.management" %%  "akka-lease-kubernetes"             % AkkaManagementVersion,
  "com.lightbend.akka.discovery"  %%  "akka-discovery-kubernetes-api"     % AkkaManagementVersion,

  "ch.qos.logback" % "logback-classic" % "1.5.32",
  "org.slf4j"      % "slf4j-api"       %  "2.0.18",

  "io.aeron" % "aeron-driver" % "1.46.9", //1.47.0
  "io.aeron" % "aeron-client" % "1.46.9",

  "org.wvlet.airframe" %% "airframe-ulid" % "2026.1.6",

  "com.lightbend.akka" %% "akka-persistence-r2dbc" % AkkaPersistenceR2dbcVersion,

  "com.lightbend.akka" %% "akka-projection-r2dbc" % AkkaProjectionVersion,
  "com.lightbend.akka" %% "akka-projection-eventsourced" % AkkaProjectionVersion,

  "org.hdrhistogram" % "HdrHistogram" % "2.2.2",
  "com.lihaoyi" % "ammonite" % AmmoniteVersion % "test" cross CrossVersion.full
)

addCommandAlias("c", "compile")
addCommandAlias("r", "reload")

enablePlugins(AkkaGrpcPlugin, JavaAppPackaging, DockerPlugin, BuildInfoPlugin)

Compile / mainClass := Some("com.resource.App")
Compile / run := Some("com.resource.App")

//dockerBaseImage := "docker.io/library/adoptopenjdk:17-jre-hotspot"

Compile / scalacOptions ++= Seq(
  //Migration mode. Preparing your code to be fully moved to Scala 3.
  //It enables Scala 3 specific syntax (like * for wildcards instead of _) and changes certain compiler behaviors to match Scala 3’s stricter rules.
  "-Xsource:3",
  //"-Xsource:3-cross",
  s"-release:$releaseJvmVersion", // tells the Scala compiler to emit bytecode that is compatible with JDK N.
  "-Wconf:msg=lambda-parens:s",
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

javaOptions ++= Seq(
  //"-XX:+PrintFlagsFinal",
  "-XX:+PrintCommandLineFlags",
  "-XshowSettings:all",

  "-Xmx256m",
  "-Xms128m",

  "-XX:+AlwaysPreTouch",
  "-XX:MaxDirectMemorySize=64m",

  // https://dzone.com/articles/troubleshooting-problems-with-native-off-heap-memo
  // To allow getting native memory stats for threads
  "-XX:NativeMemoryTracking=summary", // detail

  "-XX:ActiveProcessorCount=6",

  "-XX:+UseZGC",
  //"--add-opens",  "java.base/java.nio=ALL-UNNAMED",
  //"--add-opens",  "java.base/sun.nio.ch=ALL-UNNAMED",

  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED",
)

dockerBaseImage   := "haghard/jdk17-open-table:1.0.1" //TODO: build it for jdk21
dockerRepository    := Some("haghard")
dockerExposedPorts     := Seq(8080, 8558, 25520)
Docker / daemonUser    := "root"
Docker / daemonUserUid := None
// Publish settings
Compile / packageDoc / publishArtifact := false // speed up building Docker images
Compile / packageSrc / publishArtifact := false // speed up building Docker images
dockerUpdateLatest := true
dockerBuildxPlatforms := Seq("linux/amd64")

// make version compatible with docker for publishing
ThisBuild / dynverSeparator := "-"

buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion)
buildInfoPackage := "com.resource"
buildInfoOptions := Seq(BuildInfoOption.BuildTime)
buildInfoOptions += BuildInfoOption.BuildTime

scalafmtOnCompile := true

//run / fork := false
run / fork := true
run / connectInput := true

// Allow ctrl-c to kill forked tasks without killing SBT
Global / cancelable := true

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

