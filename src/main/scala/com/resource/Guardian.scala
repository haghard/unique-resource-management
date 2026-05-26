package com.resource

import akka.actor.RootActorPath
import akka.actor.typed.scaladsl.adapter.TypedActorSystemOps
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.*
import akka.cluster.ddata.SelfUniqueAddress
import akka.cluster.sharding.typed.ClusterShardingSettings
import akka.cluster.sharding.typed.scaladsl.*
import akka.cluster.typed.SelfUp
import akka.cluster.*
import akka.cluster.Implicits.MemberOps

import scala.collection.immutable
import scala.concurrent.duration.DurationInt
import com.resource.domain.user.*
import com.resource.domain.resource.*

import java.lang.management.ManagementFactory
import java.net.InetAddress
import java.time.LocalDateTime
import java.util.TimeZone
import scala.jdk.CollectionConverters.CollectionHasAsScala

object Guardian {

  sealed trait Protocol

  object Protocol {
    final case class ClusterViewAfterSelfUp(members: immutable.SortedSet[Member]) extends Protocol
  }

  def apply(buildVersion: String, grpcPort: Int): Behavior[Nothing] =
    Behaviors
      .setup[Protocol] { implicit ctx =>
        implicit val system                     = ctx.system
        implicit val timeout: akka.util.Timeout = akka.util.Timeout(4.seconds)
        implicit val cluster                    = akka.cluster.typed.Cluster(system)
        implicit val selfUniqueAddress          = SelfUniqueAddress(cluster.selfMember.uniqueAddress)

        val selfAddress = selfUniqueAddress.uniqueAddress.address
        ctx.log.warn("★ ★ ★  SelfUp: {}  ★ ★ ★", selfUniqueAddress)

        cluster.subscriptions.tell(
          akka.cluster.typed.Subscribe(
            ctx.messageAdapter[SelfUp] { case m: SelfUp =>
              Protocol.ClusterViewAfterSelfUp(
                immutable.SortedSet.from(m.currentClusterState.members)(Member.ageOrdering)
              )
            },
            classOf[SelfUp]
          )
        )

        Behaviors
          .receive[Protocol] { case (ctx, _ @Protocol.ClusterViewAfterSelfUp(membersByAge)) =>
            cluster.subscriptions ! akka.cluster.typed.Unsubscribe(ctx.self)

            /*val serialization = SerializationExtension(system)
            serialization.serializerOf(classOf[akka.remote.serialization.ProtobufSerializer].getName).map(_.identifier)*/

            ctx.log.warn("★ ★ ★  Up: [{}]  ★ ★ ★", membersByAge.mkString(","))

            // ClusterSingleton and ShardCoordinator run on the oldest member in the cluster
            membersByAge.headOption.foreach { shardCoordinator =>
              val jvmRuntime = scala.sys.runtime
              val jvmInfo    =
                s"Cores:${jvmRuntime.availableProcessors()} Memory:[Max=${jvmRuntime.maxMemory() / 1000000}Mb, Free=${jvmRuntime.freeMemory() / 1000000}Mb]"

              ctx.log.info(
                s"""
                   |--------------------------------------------------------------------------------
                   |ver($buildVersion)
                   |Member:${cluster.selfMember.details}🧪ShardCoordinator:${shardCoordinator.details}🧪Leader:[${cluster.state.leader
                    .getOrElse("")}]
                   |${shardCoordinator.singletonInfo}
                   |Members:[${membersByAge.map(m => s"${m.details}, v${m.appVersion}").mkString(", ")}]
                   |
                   |Env
                   |Hostname/Podname:${InetAddress.getLocalHost().getHostName()},
                   |PID:${ProcessHandle.current().pid()}. Start time:${LocalDateTime
                    .now()} / ${TimeZone.getDefault().getID()}
                   |★ ★ ★ ★ ★ ★ JVM vendor/version: ${scala.sys.props("java.vm.name")}/${scala.sys.props(
                    "java.version"
                  )} ★ ★ ★ ★ ★ ★
                   |$jvmInfo
                   |${ManagementFactory
                    .getMemoryPoolMXBeans()
                    .asScala
                    .map(p => s"${p.getName()} / ${p.getType()} / ${p.getPeakUsage()}")
                    .mkString("\n")}
                   |Args:${ManagementFactory.getRuntimeMXBean().getInputArguments()}
                   |★ ★ ★ ★ ★ ★ ★ ★ ★ ★ ★ ★
                   |👍✅🚀🧪❌😄📣🔥🐳🚨😱🥳💰⚡️🚨😱🥳
                   |---------------------------------------------------------------------------------
                   |""".stripMargin
              )

              if (cluster.selfMember.appVersion.compareTo(shardCoordinator.appVersion) > 0)
                ctx.log.info(
                  "★ ★ ★  Rolling update from {} to {}  ★ ★ ★",
                  shardCoordinator.appVersion,
                  cluster.selfMember
                )
            }

            val shardingSettings = ClusterShardingSettings(system)
            val clusterSharding  = ClusterSharding(system)

            val resources: ActorRef[ResourceCmd] =
              clusterSharding
                .init(
                  Entity(TakenUniqueResource.TypeKey)(TakenUniqueResource(_, 10))
                    .withMessageExtractor(TakenUniqueResource.Extractor(shardingSettings.numberOfShards))
                    .withStopMessage(com.resource.domain.resource.Passivate())
                    .withAllocationStrategy(utils.newLeastShardAllocationStrategy())
                )

            val userResource: ActorRef[UserCmd] =
              clusterSharding
                .init(
                  Entity(UserResourceLink.TypeKey)(UserResourceLink(_, resources))
                    .withMessageExtractor(UserResourceLink.Extractor(shardingSettings.numberOfShards))
                    .withStopMessage(com.resource.domain.user.Passivate())
                    .withAllocationStrategy(utils.newLeastShardAllocationStrategy())
                )

            val numberOfResourceUserTables = system.settings.config.getInt("number-of-resource-tables")
            val numberOfProjSlices         = system.settings.config.getInt("akka.projection.r2dbc.number-of-slices")

            // Looks up the replicator that is being used by [[akka.cluster.sharding.DDataShardCoordinator]]
            val dDataShardReplicatorPath =
              RootActorPath(system.deadLetters.path.address) / "system" / "sharding" / "replicator"

            system.toClassic
              .actorSelection(dDataShardReplicatorPath)
              .resolveOne(5.seconds)
              .foreach { dDataShardReplicator =>
                akka.cluster.utils
                  .shardingStateChanges(dDataShardReplicator, cluster.selfMember.address.host.getOrElse("local"))
              }(system.executionContext)

            val resourceTables: Vector[String] =
              (0 until numberOfResourceUserTables).map(i => s"resource_user$i").toVector
            ResourceProjection.run(resources, userResource, numberOfProjSlices, resourceTables)
            Bootstrap.run(userResource, selfAddress.host.get, grpcPort)
            Behaviors.same
          }
      }
      .narrow
}
