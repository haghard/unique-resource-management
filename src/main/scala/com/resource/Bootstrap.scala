package com.resource

import akka.Done
import akka.actor.typed.*
import akka.actor.{Address, CoordinatedShutdown}
import akka.actor.CoordinatedShutdown.*
import akka.coordination.lease.psg.PostgresLease
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.*
import com.resource.api.*
import com.resource.domain.user.*

import scala.concurrent.duration.Duration
import scala.concurrent.Future
import scala.util.*

object Bootstrap {

  private def leaseOwnerFromAkkaMember(system: ActorSystem[?], address: Address): String = {
    val sb = new java.lang.StringBuilder().append(system.name)
    address.host.foreach(h => sb.append('@').append(h))
    address.port.foreach(p => sb.append(':').append(p))
    sb.toString()
  }

  private final case object BindFailure extends Reason

  def run(
    userRequest: ActorRef[UserCmd],
    bindHost: String,
    port: Int
  )(implicit system: ActorSystem[?], reqTimeout: akka.util.Timeout): Unit = {
    import system.executionContext

    val config              = system.settings.config
    val terminationDeadline =
      Duration.fromNanos(config.getDuration("akka.coordinated-shutdown.default-phase-timeout").toNanos)

    val shutdown                                         = CoordinatedShutdown(system)
    val grpcService: HttpRequest => Future[HttpResponse] =
      ResourceServiceHandler.withServerReflection(new ResourceServiceImpl(userRequest))

    Http(system)
      .newServerAt(bindHost, port)
      .bind(grpcService)
      .onComplete {
        case Failure(ex) =>
          system.log.error(s"Shutting down because can't bind to $bindHost:$port", ex)
          shutdown.run(Bootstrap.BindFailure)
        case Success(binding) =>
          system.log.info("★ ★ ★ ★ ★ ★ ★ ★ ★ ActorSystem({}) tree ★ ★ ★ ★ ★ ★ ★ ★ ★", system.name)
          system.log.info(system.printTree)

          shutdown.addTask(PhaseBeforeServiceUnbind, "before-unbind") { () =>
            Future.successful {
              system.log.info("★ ★ ★ CoordinatedShutdown [before-unbind] ★ ★ ★")
              Done
            }
          }

          shutdown.addTask(PhaseServiceUnbind, "http-unbind") { () =>
            // No new connections are accepted. Existing connections are still allowed to perform request/response cycles
            binding.unbind().map { done =>
              system.log.info("★ ★ ★ CoordinatedShutdown [http-api.unbind] ★ ★ ★")
              done
            }
          }

          // graceful termination of requests being handled on this connection
          shutdown.addTask(PhaseServiceRequestsDone, "http-terminate") { () =>
            /** It doesn't accept new connection, but it drains the existing connections Until the `terminationDeadline`
              * all the req that have been accepted will be completed and only than the shutdown will continue
              */

            binding.terminate(terminationDeadline).map { _ =>
              system.log.info("★ ★ ★ CoordinatedShutdown [http-api.terminate]  ★ ★ ★")
              Done
            }
          }

          // forcefully kills connections that are still open
          shutdown.addTask(PhaseServiceStop, "close.connections") { () =>
            Http().shutdownAllConnectionPools().map { _ =>
              system.log.info("★ ★ ★ CoordinatedShutdown [close.connections] ★ ★ ★")
              Done
            }
          }

          if (
            system.settings.config.getString(
              "akka.cluster.split-brain-resolver.lease-majority.lease-implementation"
            ) == PostgresLease.configPath
          ) {
            // best-effort attempt to clean up sbr_lease
            shutdown.addTask(PhaseClusterExitingDone, "release.lease") { () =>
              val sua        = akka.cluster.Cluster(system).selfUniqueAddress
              val leaseOwner = leaseOwnerFromAkkaMember(system, sua.address)
              val lease      = akka.coordination.lease.scaladsl
                .LeaseProvider(system)
                .getLease(
                  s"${App.AkkaSystemName}-akka-${PostgresLease.SbrPref}",
                  PostgresLease.configPath,
                  leaseOwner
                )

              lease match {
                case lease: PostgresLease =>
                  system.log.info(
                    s"★ ★ ★ CoordinatedShutdown [release.lease] by $leaseOwner released on exit: {} ★ ★ ★"
                  )
                  lease.releaseOnExit().map(_ => Done)
                case _ =>
                  Future.successful(Done)
              }
            }
          }

          shutdown.addTask(PhaseActorSystemTerminate, "system.term") { () =>
            Future.successful {
              system.log.info("★ ★ ★ CoordinatedShutdown [close.connections] ★ ★ ★")
              Done
            }
          }
      }
  }
}
