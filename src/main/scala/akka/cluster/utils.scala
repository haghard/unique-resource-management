package akka
package cluster

import akka.actor.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.adapter.TypedActorRefOps
import akka.cluster.ddata.{LWWRegister, LWWRegisterKey, Replicator}
import akka.cluster.sharding.ShardCoordinator
import akka.cluster.sharding.ShardCoordinator.ShardAllocationStrategy
import akka.stream.*
import akka.stream.scaladsl.*
import akka.stream.typed.scaladsl.ActorSource
import com.resource.TakenUniqueResource

object utils {

  def newLeastShardAllocationStrategy() = {
    val leastShardAllocationNew: akka.cluster.sharding.internal.LeastShardAllocationStrategy =
      ShardAllocationStrategy
        .leastShardAllocationStrategy(3, 1)
        .asInstanceOf[akka.cluster.sharding.internal.LeastShardAllocationStrategy]
    leastShardAllocationNew
  }

  val typeName: String    = TakenUniqueResource.TypeKey.name
  val CoordinatorStateKey = LWWRegisterKey[ShardCoordinator.Internal.State](s"${typeName}CoordinatorState")

  def shardingStateChanges(dDataShardReplicator: ActorRef, selfHost: String)(implicit
    sys: ActorSystem[_]
  ): KillSwitch = {
    val actorWatchingFlow =
      Flow[String]
        .watch(dDataShardReplicator)
        .buffer(1, OverflowStrategy.backpressure)

    type ShardCoordinatorState = LWWRegister[akka.cluster.sharding.ShardCoordinator.Internal.State]
    val (actorSource, src) =
      ActorSource
        .actorRef[Replicator.SubscribeResponse[ShardCoordinatorState]](
          completionMatcher = { case _: Replicator.Deleted[ShardCoordinatorState] =>
            CompletionStrategy.draining
          },
          failureMatcher = PartialFunction.empty,
          1,
          OverflowStrategy.dropHead
        )
        .preMaterialize()

    dDataShardReplicator ! Replicator.Subscribe(CoordinatorStateKey, actorSource.toClassic)

    src
      .collect { case value @ Replicator.Changed(_) =>
        val shardCoordinatorState: ShardCoordinator.Internal.State = value.get(CoordinatorStateKey).value
        new StringBuilder()
          .append("\n")
          // .append("Shards: [")
          // .append(state.shards.keySet.mkString(","))
          // .append(state.shards.mkString(","))
          // .append(state.shards.map { case (k, ar) => s"$k:${ar.path.address.host.getOrElse(selfHost)}" }.mkString(","))
          // .append("]")
          // .append("\n")
          .append(s"ShardCoordinatorState($selfHost) updated [ ")
          .append(
            shardCoordinatorState.regions
              .map { case (sr, shards) => s"${sr.path.address.host.getOrElse(selfHost)}:[${shards.mkString(",")}]" }
              .mkString(", ")
          )
          .append(" ]")
          .toString()
      }
      .via(actorWatchingFlow)
      .viaMat(KillSwitches.single)(Keep.right)
      .to(Sink.foreach(stateLine => sys.log.warn(stateLine)))
      .withAttributes(
        ActorAttributes.supervisionStrategy {
          case ex: akka.stream.WatchedActorTerminatedException =>
            sys.log.error("Replicator failed. Terminate stream", ex)
            Supervision.Stop
          case ex: Throwable =>
            sys.log.error("Unexpected error!", ex)
            Supervision.Stop
        }
      )
      .run()
  }
}
