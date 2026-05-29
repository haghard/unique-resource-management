package com.resource

import akka.actor.typed.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}

object App extends Ops {

  val AkkaSystemName = "resources"
  val GRPC_PORT_VAR  = "GRPC_PORT"

  def onKubernetes(): Boolean =
    sys.env.contains("KUBERNETES_PORT")

  def main(args: Array[String]): Unit = {

    sys.props += "APP_VERSION_VAR" -> com.resource.BuildInfo.version
    sys.props += "slf4j.provider"  -> classOf[ch.qos.logback.classic.spi.LogbackServiceProvider].getName

    val opts: Map[String, String] = argsToOpts(args.toList)
    applySystemProperties(opts)

    val akkaPort = sys.props
      .get("akka.remote.artery.canonical.port")
      .flatMap(_.toIntOption)
      .getOrElse(throw new Exception("akka.remote.artery.canonical.port not found"))

    val grpcPort = sys.props
      .get(GRPC_PORT_VAR)
      .flatMap(_.toIntOption)
      .getOrElse(throw new Exception(s"$GRPC_PORT_VAR not found"))

    val config =
      if (onKubernetes()) {
        applySystemProperties(
          Map(
            "-Dakka.persistence.r2dbc.connection-factory.ssl.enabled" -> "true",
            "-Dakka.persistence.r2dbc.connection-factory.ssl.mode"    -> "require"
          )
        )
        ConfigFactory.load()
      } else {

        val hostNameFromEnvVar = sys.props.get("akka.remote.artery.canonical.hostname")
        val dockerHostName     = internalDockerAddr
          .map(_.getHostAddress)
          .orElse(hostNameFromEnvVar)
          .getOrElse(throw new Exception("akka.remote.artery.canonical.hostname is expected"))

        applySystemProperties(
          Map(
            "-Dakka.management.http.hostname"      -> dockerHostName,
            "-Dakka.remote.artery.bind.hostname"   -> dockerHostName,
            "-Dakka.management.http.bind-hostname" -> dockerHostName,
            "-Dakka.remote.artery.bind.port"       -> akkaPort.toString
          )
        )

        val config: Config = {

          val bootstrapEndpoints = {
            val CONTACT_POINTS_VAR = "CONTACT_POINTS"
            val contactPoints      =
              sys.props
                .get(CONTACT_POINTS_VAR)
                .map(_.split(","))
                .getOrElse(throw new Exception(s"$CONTACT_POINTS_VAR not found"))

            if (contactPoints.isEmpty)
              throw new Exception(s"$CONTACT_POINTS_VAR must not be empty")

            val endpointList = contactPoints.map(s => s"{host=$s,port=8558}").mkString(",")
            ConfigFactory
              .parseString(s"akka.discovery.config.services { $AkkaSystemName = { endpoints = [ $endpointList ] }}")
              .resolve()
          }
          bootstrapEndpoints
            .withFallback(ConfigFactory.load())
        }

        config
      }

    val buildVersion = s"${com.resource.BuildInfo.version} built at ${com.resource.BuildInfo.builtAtString}"
    val system       = ActorSystem[Nothing](Guardian(buildVersion, grpcPort), AkkaSystemName, config)
    akka.management.scaladsl.AkkaManagement(system).start()
    akka.management.cluster.bootstrap.ClusterBootstrap(system).start()

    /*
    akka.discovery.Discovery(system).loadServiceDiscovery("config")
    akka.discovery.Discovery(system).loadServiceDiscovery("kubernetes-api")
     */

    // TODO: for local debug only !!!!!!!!!!!!!!!!!!!
    /*val _ = scala.io.StdIn.readLine()
    system.log.warn("★ ★ ★ ★ ★ ★  Shutting down ... ★ ★ ★ ★ ★ ★")
    system.terminate()
    scala.concurrent.Await.result(
      system.whenTerminated,
      scala.concurrent.duration
        .DurationLong(
          config
            .getDuration("akka.coordinated-shutdown.default-phase-timeout", java.util.concurrent.TimeUnit.SECONDS)
        )
        .seconds
    )*/
  }
}
