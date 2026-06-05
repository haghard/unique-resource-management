package com.resource

import akka.actor.typed.ActorSystem
import com.typesafe.config.ConfigFactory
import kamon.Kamon

object App extends Ops {

  val AkkaSystemName = "resources"

  def main(args: Array[String]): Unit = {
    Kamon.init()

    sys.props += "slf4j.provider" -> classOf[ch.qos.logback.classic.spi.LogbackServiceProvider].getName

    val config =
      ConfigFactory
        .parseString(s"akka.cluster.app-version=${com.resource.BuildInfo.version}")
        .resolve()
        .withFallback(ConfigFactory.load())

    val system = ActorSystem[Nothing](Guardian(config.getInt("grpc.port")), AkkaSystemName, config)
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
