package com.resource

import akka.actor.typed.*
import akka.actor.typed.scaladsl.*
import akka.cluster.sharding.typed.ShardingMessageExtractor
import akka.cluster.sharding.typed.scaladsl.*
import akka.pattern.StatusReply
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.state.*
import akka.persistence.typed.state.scaladsl.*
import com.resource.Implicits.ResourceOps
import com.resource.api.*
import com.resource.domain.*
import com.resource.domain.resource.ResourceCmdMessage
import com.resource.domain.user.*
import org.slf4j.Logger

import scala.concurrent.duration.*

/** [[UserResourceLink]] Acts as a per-user request latch to prevent concurrent requests from multiple clients. Requests
  * from different users can be served in parallel.
  */
object UserResourceLink {

  val TypeKey: EntityTypeKey[UserCmd] = EntityTypeKey[UserCmd](name = "usr-rs")

  object Extractor {
    def apply(numberOfShards: Int): ShardingMessageExtractor[UserCmd, UserCmd] =
      new ShardingMessageExtractor[UserCmd, UserCmd] {
        override def entityId(cmd: UserCmd): String =
          cmd match {
            case c: GetResource =>
              c.userId
            case c: Assign =>
              c.userId
            case c: Reassign =>
              c.userId
            case c: Release =>
              c.userId
            case c: Confirm =>
              c.userId
            case com.resource.domain.user.Passivate() | RedeliveryTick() | UserCmd.Empty =>
              throw new Exception(s"Unsupported cmd")
          }

        override def shardId(ownerId: String): String =
          math.abs(ownerId.hashCode % numberOfShards).toString

        override def unwrapMessage(cmd: UserCmd): UserCmd = cmd
      }
  }

  def apply(
    entityCtx: EntityContext[UserCmd],
    uniqueResources: ActorRef[resource.ResourceCmd]
  ): Behavior[UserCmd] =
    Behaviors.setup { implicit ctx =>
      Behaviors.withTimers { implicit timer =>
        val userId                                 = ctx.self.path.elements.last
        implicit val refResolver: ActorRefResolver = ActorRefResolver(ctx.system)
        val redeliverAfter: FiniteDuration         = 6.seconds

        DurableStateBehavior
          .withEnforcedReplies[UserCmd, UserResourceState](
            PersistenceId(TypeKey.name, entityCtx.entityId),
            UserResourceState(),
            (state, cmd) => state.applyCmd(userId, cmd, uniqueResources, redeliverAfter)
          )
          .receiveSignal {
            case (state, RecoveryCompleted) =>
              ctx.log.warn(s"★★★ RecoveryCompleted: ${state.pbState.lockState}")
              state.redeliver(userId, uniqueResources, redeliverAfter)(timer, ctx.log)
            case (_, RecoveryFailed(cause)) =>
              ctx.log.error(s"There is a problem with state recovery $cause", cause)
          }
          .onPersistFailure(
            SupervisorStrategy.restartWithBackoff(minBackoff = 3.seconds, maxBackoff = 10.seconds, randomFactor = 0.3)
          )
      }
    }

  implicit class UserResourceLinkOps(val pbState: UserResourceState) extends AnyVal {

    def redeliver(
      userId: String,
      uniqueResources: ActorRef[resource.ResourceCmd],
      redeliverAfter: FiniteDuration
    )(implicit timer: TimerScheduler[UserCmd], logger: Logger): Unit =
      pbState.lockState.foreach { ls =>
        uniqueResources.tell(ls.pendingCmd)
        logger.info("Redeliver {}", ls.pendingCmd)
        timer.startSingleTimer(userId + "@" + ls.pendingCmd, RedeliveryTick(), redeliverAfter)
      }

    def applyCmd(
      userId: String,
      cmd: UserCmd,
      uniqueResources: ActorRef[resource.ResourceCmd],
      redeliverAfter: FiniteDuration
    )(implicit
      ctx: ActorContext[UserCmd],
      refResolver: ActorRefResolver,
      timer: TimerScheduler[UserCmd]
    ): ReplyEffect[UserResourceState] = {
      implicit val logger = ctx.log
      cmd match {
        case GetResource(_, replyTo) =>
          pbState.linkedResource match {
            case Some(linked) =>
              Effect.none
                .thenReply(refResolver.resolveActorRef(replyTo)) { _: UserResourceState =>
                  StatusReply.success(
                    GetResourceReply(Some(linked.location), Some(linked.resource))
                  )
                }

            case None =>
              Effect.none
                .thenReply(refResolver.resolveActorRef(replyTo)) { _: UserResourceState =>
                  StatusReply.success(GetResourceReply(None, None))
                }
          }

        case assign @ Assign(userId, resourceToAssign, grpcClient) =>
          pbState.lockState match {
            case Some(_) =>
              Effect.none
                .thenReply(refResolver.resolveActorRef(grpcClient)) { _: UserResourceState =>
                  StatusReply.success(
                    ResourceReply(userId, ResourceReply.StatusCode.Aborted, ResourceLocation(-1, -1))
                  )
                }

            case None =>
              pbState.linkedResource match {
                case Some(linked) =>
                  Effect.none
                    .thenReply(refResolver.resolveActorRef(grpcClient)) { _: UserResourceState =>
                      val reply =
                        if (linked.resource == assign.resource) ResourceReply.StatusCode.OKNoOp
                        else ResourceReply.StatusCode.AnotherResourceFound
                      StatusReply.success(ResourceReply(assign.userId, reply, linked.location))
                    }

                case None =>
                  val cmdSeqNum = DurableStateBehavior.lastSequenceNumber(ctx)
                  ctx.log.warn(
                    s"Lock [$userId/${ResponseTag.Assigned}/$cmdSeqNum = ${resourceToAssign.uniqueKey}]"
                  )
                  val pendingCmd =
                    resource.Assign(
                      assign.userId,
                      resourceToAssign,
                      cmdSeqNum,
                      refResolver.toSerializationFormat(ctx.self)
                    )

                  val updatedState = pbState.update(_.lockState := LockState(pendingCmd, grpcClient))
                  Effect
                    .persist(updatedState)
                    .thenRun { _: UserResourceState =>
                      uniqueResources.tell(pendingCmd)
                      timer.startSingleTimer(userId + "@" + cmdSeqNum, RedeliveryTick(), redeliverAfter)
                    }
                    .thenNoReply()
              }
          }

        case Release(userId, location, grpcClient) =>
          pbState.lockState match {
            case Some(_) =>
              Effect.none
                .thenReply(refResolver.resolveActorRef(grpcClient)) { _: UserResourceState =>
                  StatusReply.success(
                    ResourceReply(userId, ResourceReply.StatusCode.Aborted, ResourceLocation(-1, -1))
                  )
                }

            case None =>
              pbState.linkedResource match {
                case Some(linked) =>
                  if (linked.location == location) {
                    val cmdSeqNum = DurableStateBehavior.lastSequenceNumber(ctx)
                    ctx.log.warn(s"Lock [$userId/${ResponseTag.Unassigned}/$cmdSeqNum = ${linked.resource.uniqueKey}]")

                    val pc =
                      resource.Release(
                        userId,
                        linked.resource,
                        linked.location,
                        cmdSeqNum,
                        refResolver.toSerializationFormat(ctx.self)
                      )

                    val updatedState = pbState.update(_.lockState := LockState(pendingCmd = pc, grpcClient))

                    Effect
                      .persist(updatedState)
                      .thenRun { _: UserResourceState =>
                        uniqueResources.tell(pc)
                        timer.startSingleTimer(userId + "@" + cmdSeqNum, RedeliveryTick(), redeliverAfter)
                      }
                      .thenNoReply()
                  } else {
                    Effect.none
                      .thenReply(refResolver.resolveActorRef(grpcClient)) { _: UserResourceState =>
                        StatusReply.success(
                          ResourceReply(userId, ResourceReply.StatusCode.LocationNotFound, location)
                        )
                      }
                  }
                case None =>
                  Effect.none
                    .thenReply(refResolver.resolveActorRef(grpcClient)) { _: UserResourceState =>
                      StatusReply.success(
                        ResourceReply(userId, ResourceReply.StatusCode.OKNoOp, location)
                      )
                    }
              }
          }

        case reassign @ Reassign(userId, resourceToAssign, location, grpcClient) =>
          pbState.lockState match {
            case Some(_) =>
              Effect.none
                .thenReply(refResolver.resolveActorRef(grpcClient)) { _: UserResourceState =>
                  StatusReply.success(
                    ResourceReply(userId, ResourceReply.StatusCode.Aborted, location)
                  )
                }

            case None =>
              pbState.linkedResource match {
                case Some(linked) =>
                  if (linked.location == location) {
                    if (linked.resource.uniqueKey == resourceToAssign.uniqueKey) {
                      Effect.none
                        .thenReply(refResolver.resolveActorRef(grpcClient)) { _: UserResourceState =>
                          StatusReply.success(
                            ResourceReply(userId, ResourceReply.StatusCode.OKNoOp, linked.location)
                          )
                        }
                    } else {
                      val cmdSeqNum = DurableStateBehavior.lastSequenceNumber(ctx)
                      ctx.log.warn(
                        s"Lock [$userId/${ResponseTag.Reassigned}/$cmdSeqNum = ${resourceToAssign.uniqueKey}]"
                      )

                      val pc =
                        resource.Reassign(
                          reassign.userId,
                          resourceToAssign,
                          linked.location,
                          cmdSeqNum,
                          refResolver.toSerializationFormat(ctx.self)
                        )

                      val updatedState = pbState.update(_.lockState := LockState(pc, grpcClient))
                      Effect
                        .persist(updatedState)
                        .thenRun { _: UserResourceState =>
                          uniqueResources.tell(pc)
                          timer.startSingleTimer(userId + "@" + cmdSeqNum, RedeliveryTick(), redeliverAfter)
                        }
                        .thenNoReply()
                    }
                  } else {
                    Effect.none.thenReply(refResolver.resolveActorRef(grpcClient)) { _: UserResourceState =>
                      StatusReply.success(
                        ResourceReply(userId, ResourceReply.StatusCode.LocationNotFound, linked.location)
                      )
                    }
                  }

                case None =>
                  Effect.none
                    .thenReply(refResolver.resolveActorRef(grpcClient)) { _: UserResourceState =>
                      StatusReply.success(
                        ResourceReply(userId, ResourceReply.StatusCode.UpdateFailure, location)
                      )
                    }
              }
          }

        case Confirm(_, resource, location, requestTag, cmdSeqNum, projection) =>
          val projectionToReply = refResolver.resolveActorRef(projection)

          pbState.lockState match {
            case Some(ls) =>

              val (pendingCmdSeqNum, grpcClient) =
                ls.pendingCmd.asMessage.sealedValue match {
                  case ResourceCmdMessage.SealedValue.Assign(assign) =>
                    (assign.pendingCmdSeqNum, ls.grpcClient)
                  case ResourceCmdMessage.SealedValue.Release(release) =>
                    (release.pendingCmdSeqNum, ls.grpcClient)
                  case ResourceCmdMessage.SealedValue.Reassign(reassign) =>
                    (reassign.pendingCmdSeqNum, ls.grpcClient)
                  case other =>
                    throw new Exception(s"Unexpected pending cmd: $other")
                }

              if (pendingCmdSeqNum == cmdSeqNum) {

                val (updatedState, statusReply) =
                  requestTag match {
                    case ResponseTag.Assigned | ResponseTag.Reassigned =>
                      (
                        pbState.update(
                          _.optionalLockState      := None,
                          _.optionalLinkedResource := Some(LinkedResource(resource, location))
                        ),
                        StatusReply.success(ResourceReply(userId, ResourceReply.StatusCode.OK, location))
                      )
                    case ResponseTag.Unassigned =>
                      pbState.clearLockState.clearLinkedResource -> StatusReply.success(
                        ResourceReply(userId, ResourceReply.StatusCode.OK, location)
                      )

                    case ResponseTag.ReservedByAnotherUser =>
                      pbState.clearLockState -> StatusReply.success(
                        ResourceReply(userId, ResourceReply.StatusCode.ReservedByAnotherUser, location)
                      )
                    case ResponseTag.Unrecognized(_) | ResponseTag.Unspecified =>
                      pbState -> StatusReply.error("Unknown " + classOf[ResponseTag].getName)
                  }

                Effect
                  .persist(updatedState)
                  .thenRun { _ =>
                    ctx.log.warn(s"Unlock [$userId/$requestTag/$cmdSeqNum = ${resource.uniqueKey}]")
                    if (projection.nonEmpty) {
                      ctx.log.info(s"Confirm $cmdSeqNum")
                      projectionToReply.tell(StatusReply.success(akka.Done))
                    }
                  }
                  .thenReply(refResolver.resolveActorRef(grpcClient)) { _: UserResourceState =>
                    ctx.log.info(s"Released ${requestTag.name}-lock / ${updatedState.linkedResource.getOrElse("")}")
                    statusReply
                  }
              } else if (pendingCmdSeqNum > cmdSeqNum) {
                Effect.none
                  .thenRun { _: UserResourceState =>
                    if (projection.nonEmpty) {
                      ctx.log.info(s"Reconfirm old cmd $cmdSeqNum")
                      projectionToReply.tell(StatusReply.success(akka.Done))
                    }
                  }
                  .thenNoReply()
              } else {
                Effect.none
                  .thenRun { _: UserResourceState =>
                    ctx.log.warn(s"Got Confirm(${cmdSeqNum}) but ${pendingCmdSeqNum} expected")
                  }
                  .thenNoReply()
              }

            case None =>
              Effect.none
                .thenRun { _: UserResourceState =>
                  if (projection.nonEmpty) {
                    ctx.log.info(s"Reconfirm(${cmdSeqNum})")
                    projectionToReply.tell(StatusReply.success(akka.Done))
                  }
                }
                .thenNoReply()
          }

        case RedeliveryTick() =>
          Effect
            .none[UserResourceState]
            .thenRun { _ =>
              pbState.redeliver(userId, uniqueResources, redeliverAfter)
            }
            .thenNoReply()

        case user.Passivate() =>
          Effect
            .none[UserResourceState]
            .thenRun(_ => ctx.log.info(s"Passivate"))
            .thenStop()
            .thenNoReply()

        case user.UserCmd.Empty =>
          Effect
            .none[UserResourceState]
            .thenRun(_ => ctx.log.info(s"Got Empty"))
            .thenStop()
            .thenNoReply()
      }
    }
  }
}
