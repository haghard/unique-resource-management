package com.resource

import akka.Done
import akka.actor.typed.*
import akka.actor.typed.scaladsl.*
import akka.cluster.sharding.typed.ShardingMessageExtractor
import akka.cluster.sharding.typed.scaladsl.*
import akka.pattern.StatusReply
import akka.persistence.typed.*
import akka.persistence.typed.scaladsl.*

import scala.concurrent.duration.DurationInt
import com.resource.domain.*
import com.resource.domain.user.Confirm
import Implicits.*
import com.resource.domain.resource.*

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

object TakenUniqueResource {

  val TypeKey: EntityTypeKey[ResourceCmd] = EntityTypeKey[ResourceCmd](name = "rs")

  object Extractor {
    def apply(numberOfShards: Int): ShardingMessageExtractor[ResourceCmd, ResourceCmd] =
      new ShardingMessageExtractor[ResourceCmd, ResourceCmd] {
        override def entityId(cmd: ResourceCmd): String =
          cmd match {
            case c: resource.Assign =>
              val bts = ByteBuffer.wrap(c.resource.uniqueKey.getBytes(StandardCharsets.UTF_8))
              CassandraMurmurHash.hash2_64(bts, 0, bts.array.length, akka.util.HashCode.SEED).toString
            case c: resource.Release =>
              val bts = ByteBuffer.wrap(c.resource.uniqueKey.getBytes(StandardCharsets.UTF_8))
              CassandraMurmurHash.hash2_64(bts, 0, bts.array.length, akka.util.HashCode.SEED).toString
            case c: resource.Reassign =>
              val bts = ByteBuffer.wrap(c.resource.uniqueKey.getBytes(StandardCharsets.UTF_8))
              CassandraMurmurHash.hash2_64(bts, 0, bts.array.length, akka.util.HashCode.SEED).toString
            case c: resource.Unassign =>
              c.unassignedLocation.bucketId.toString
            case com.resource.domain.resource.Passivate() | ResourceCmd.Empty =>
              throw new Exception(s"Unsupported cmd")
          }

        override def shardId(entityId: String): String =
          math.abs(entityId.toLong % numberOfShards).toString

        override def unwrapMessage(cmd: ResourceCmd): ResourceCmd = cmd
      }
  }

  def apply(entityCtx: EntityContext[ResourceCmd], snapshotEveryNEvents: Int): Behavior[ResourceCmd] =
    Behaviors.setup { implicit ctx =>
      val entityId                            = ctx.self.path.elements.last.toLong
      implicit val resolver: ActorRefResolver = ActorRefResolver(ctx.system)

      // val usrRes: ActorRef[user.UserCmd] = ???
      EventSourcedBehavior
        .withEnforcedReplies[ResourceCmd, ResourceEvent, TakenResourceState](
          PersistenceId(TypeKey.name, entityCtx.entityId),
          TakenResourceState(),
          (state, cmd) => state.applyCmd(cmd, entityId),
          (state, event) => state.applyEvt(event)
        )
        .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = snapshotEveryNEvents, keepNSnapshots = 2))
        .receiveSignal {
          case (state, RecoveryCompleted) =>
            ctx.log.warn(s"★★★ RecoveryCompleted: - ${state.contentKeySeqNum.size}")
          case (state, SnapshotCompleted(_)) =>
            ctx.log.info(s"★★★ SnapshotCompleted: ${state.contentKeySeqNum.size}")
          case (state, SnapshotFailed(_, ex)) =>
            ctx.log.error(s"★★★ Saving snapshot $state failed", ex)
          case (_, RecoveryFailed(cause)) =>
            ctx.log.error(s"There is a problem with state recovery $cause", cause)
        }
        .onPersistFailure(
          SupervisorStrategy.restartWithBackoff(minBackoff = 3.seconds, maxBackoff = 10.seconds, randomFactor = 0.3)
        )
    }

  implicit class TakenUniqueResourceOps(val pbState: TakenResourceState) extends AnyVal {
    def applyCmd(
      cmd: ResourceCmd,
      entityId: Long
    )(implicit
      ctx: ActorContext[ResourceCmd],
      resolver: ActorRefResolver
    ): ReplyEffect[ResourceEvent, TakenResourceState] =
      cmd match {
        case resource.Assign(userId, resource, pendingCmdSeqNum, replyTo) =>
          val userResourceLinkRef = resolver.resolveActorRef(replyTo)
          ctx.log.info(s"★★★ Assign(${resource.uniqueKey}), user:$userId / $userResourceLinkRef")
          // Thread.sleep(800) // for local testing

          /*if (scala.util.Random.nextDouble() < .6) {
            Effect.none
              .thenRun { _: TakenResourceState => println("DROP Assign") }
              .thenNoReply()
          } else {*/

          pbState.contentKeySeqNum.get(resource.uniqueKey) match {
            case Some(seqNum) =>

              /*

              Effect.none
                .thenRun { _: TakenResourceState =>
                  ClusterSharding(ctx.system).entityRefFor(UserResourceLink.TypeKey, userId)
                }
                .thenNoReply()*/

              Effect.none
                .thenReply(userResourceLinkRef) { _: TakenResourceState =>
                  val responseTag =
                    if (pbState.userId.exists(_ == userId)) ResponseTag.Assigned
                    else ResponseTag.ReservedByAnotherUser
                  Confirm(
                    userId,
                    resource,
                    ResourceLocation(entityId, seqNum),
                    responseTag,
                    pendingCmdSeqNum
                  )
                }

            case None =>
              val eventSeqNum = EventSourcedBehavior.lastSequenceNumber(ctx)
              Effect
                .persist(Assigned(userId, resource, eventSeqNum, None, pendingCmdSeqNum))
                .thenRun { _: TakenResourceState =>
                  ctx.log.info(s"User:$userId assigned ${resource.uniqueKey}")
                }
                .thenNoReply()
          }

        case resource.Release(userId, releasedResource, releasedLocation, pendingCmdSeqNum, replyTo) =>
          ctx.log.info(s"★★★ Release(${releasedResource.uniqueKey}), user:$userId")
          pbState.contentKeySeqNum.collectFirst {
            case (_, seqNum) if seqNum == releasedLocation.seqNum => seqNum
          } match {
            case Some(_) =>
              Effect
                .persist(
                  Unassigned(
                    userId,
                    releasedLocation,
                    releasedResource,
                    pendingCmdSeqNum
                  )
                )
                .thenRun { _: TakenResourceState =>
                  ctx.log.info(s"★★★ Got Release(${releasedResource.uniqueKey}), user:$userId")
                }
                .thenNoReply()

            case None =>
              val userResourceLinkRef = resolver.resolveActorRef(replyTo)
              Effect.none
                .thenReply(userResourceLinkRef) { _: TakenResourceState =>
                  Confirm(userId, releasedResource, releasedLocation, ResponseTag.Unassigned, pendingCmdSeqNum)
                }
          }

        case resource.Reassign(userId, resource, releasedLocation, pendingCmdSeqNum, replyTo) =>
          val userResourceLinkRef = resolver.resolveActorRef(replyTo)
          ctx.log.info(s"★★★ Reassign(${resource.uniqueKey}), user:$userId")
          pbState.contentKeySeqNum.get(resource.uniqueKey) match {
            case Some(seqNum) =>
              Effect.none
                .thenReply(userResourceLinkRef) { _: TakenResourceState =>
                  val responseTag =
                    if (pbState.userId.exists(_ == userId)) ResponseTag.Reassigned
                    else ResponseTag.ReservedByAnotherUser

                  Confirm(
                    userId,
                    resource,
                    ResourceLocation(entityId, seqNum),
                    responseTag,
                    pendingCmdSeqNum
                  )
                }

            case None =>
              val eventSeqNum = EventSourcedBehavior.lastSequenceNumber(ctx)
              Effect
                .persist(Assigned(userId, resource, eventSeqNum, Some(releasedLocation), pendingCmdSeqNum))
                .thenNoReply()
          }

        case resource.Unassign(
              userId,
              resource,
              assignedLocation,
              unassignedLocation,
              pendingCmdSeqNum,
              projection
            ) =>
          pbState.contentKeySeqNum.collectFirst {
            case (_, seqNum) if seqNum == unassignedLocation.seqNum => seqNum
          } match {
            case Some(seqNum) =>
              Effect
                .persist(
                  Released(
                    userId,
                    unassignedLocation,
                    resource,
                    assignedLocation,
                    pendingCmdSeqNum
                  )
                )
                .thenReply(resolver.resolveActorRef(projection)) { _ =>
                  ctx.log.info(s"Unassign($userId:$seqNum)")
                  StatusReply.success(Done)
                }

            case None =>
              Effect
                .none[ResourceEvent, TakenResourceState]
                .thenReply(resolver.resolveActorRef(projection)) { _: TakenResourceState =>
                  ctx.log.info(s"Failed to Unassign. $userId: Not found")
                  StatusReply.success(Done)
                }
          }

        case resource.Passivate() =>
          Effect
            .none[ResourceEvent, TakenResourceState]
            .thenRun(_ => ctx.log.info(s"Passivated: ${pbState.contentKeySeqNum.size}"))
            .thenStop()
            .thenNoReply()

        case ResourceCmd.Empty =>
          Effect
            .none[ResourceEvent, TakenResourceState]
            .thenRun(_ => ctx.log.error(s"Got Empty"))
            .thenStop()
            .thenNoReply()
      }

    def applyEvt(event: ResourceEvent)(implicit ctx: ActorContext[ResourceCmd]): TakenResourceState =
      event match {
        case Assigned(userId, resource, seqNum, _, _) =>
          ctx.log.info("Acquired({}) by {}/{}", resource.uniqueKey, userId, seqNum)
          val updatedMap = pbState.contentKeySeqNum + (resource.uniqueKey -> seqNum)
          pbState
            .update(
              _.contentKeySeqNum := updatedMap,
              _.optionalUserId   := Some(userId)
            )

        case Unassigned(userId, releasedLocation, _, _) =>
          pbState.contentKeySeqNum
            .collectFirst {
              case (resourceKey, seqNum) if seqNum == releasedLocation.seqNum =>
                resourceKey
            } match {
            case Some(resourceKey) =>
              ctx.log.info("Unassigned({}) {}/{}", resourceKey, userId, releasedLocation.seqNum)
              val updated = pbState.contentKeySeqNum - resourceKey
              pbState.update(
                _.contentKeySeqNum := updated,
                _.optionalUserId   := None
              )
            case None =>
              pbState
          }

        case Released(userId, releasedLocation, _, _, _) =>
          pbState.contentKeySeqNum
            .collectFirst {
              case (resourceKey, seqNum) if seqNum == releasedLocation.seqNum =>
                resourceKey
            } match {
            case Some(resourceKey) =>
              ctx.log.info("Released({}) {}/{}", resourceKey, userId, releasedLocation.seqNum)
              val updated = pbState.contentKeySeqNum - resourceKey
              pbState.update(
                _.contentKeySeqNum := updated,
                _.optionalUserId   := None
              )
            case None =>
              pbState
          }

        case com.resource.domain.resource.ResourceEvent.Empty =>
          pbState
      }
  }
}
