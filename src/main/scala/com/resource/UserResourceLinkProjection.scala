package com.resource

import akka.Done
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.actor.typed.{ActorRef, ActorRefResolver, ActorSystem}
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.cluster.utils
import akka.persistence.query.*
import akka.persistence.r2dbc.state.scaladsl.R2dbcDurableStateStore
import akka.projection.{Projection, ProjectionBehavior, ProjectionId}
import akka.projection.r2dbc.scaladsl.{R2dbcProjection, R2dbcSession}
import akka.projection.scaladsl.SourceProvider
import akka.projection.state.scaladsl.DurableStateSourceProvider
import com.resource.domain.resource
import com.resource.domain.resource.{ReassignReply, ReleaseReply, ResourceCmd, ResourceCmdMessage}
import com.resource.domain.user.*

import scala.concurrent.Future

object UserResourceLinkProjection {

  val entityName = UserResource.TypeKey.name

  private def sourceProvider(
    sliceRange: Range
  )(implicit system: ActorSystem[?]): SourceProvider[Offset, DurableStateChange[UserResourceState]] =
    DurableStateSourceProvider.changesBySlices(
      system,
      R2dbcDurableStateStore.Identifier,
      entityName,
      sliceRange.min,
      sliceRange.max
    )

  def run(
    takenResources: ActorRef[ResourceCmd],
    userResourceLinks: ActorRef[UserCmd],
    numberOfSlices: Int,
    askTimeout: akka.util.Timeout
  )(implicit system: ActorSystem[?]) = {
    val projectionName = entityName + "-proj"
    implicit val to    = askTimeout

    def projection(sliceRange: Range): Projection[DurableStateChange[UserResourceState]] = {
      implicit val scheduler         = system.scheduler
      implicit val ec                = system.executionContext
      val minSlice                   = sliceRange.min
      val maxSlice                   = sliceRange.max
      val projectionId               = ProjectionId(projectionName, s"$minSlice-$maxSlice")
      val resolver: ActorRefResolver = ActorRefResolver(system)
      R2dbcProjection.exactlyOnce(
        projectionId,
        settings = None,
        sourceProvider(sliceRange),
        handler = () => {
          // (change: DurableStateChange[UserResourceState]) =>
          (_: R2dbcSession, change: DurableStateChange[UserResourceState]) =>
            change match {
              case c: UpdatedDurableState[UserResourceState] =>
                c.value.lockState match {
                  case Some(ls) =>
                    println("1. UserResourceLinkProjection: Lock " + c.toString + " at " + System.currentTimeMillis())
                    ls.pendingCmd.asMessage.sealedValue match {
                      case ResourceCmdMessage.SealedValue.Assign(assign) =>
                        takenResources
                          .ask[resource.AssignReply] { replyTo =>
                            resource.Assign(
                              assign.userId,
                              assign.resource,
                              assign.pendingCmdSeqNum,
                              resolver.toSerializationFormat(replyTo)
                            )
                          }
                          .flatMap { reply =>
                            println(
                              "4. UserResourceLinkProjection: " + reply.responseTag + " at " + System
                                .currentTimeMillis()
                            )
                            if (reply.responseTag.isAssignAccepted)
                              Future.successful(Done)
                            else {
                              userResourceLinks.askWithStatus[Done] { replyTo =>
                                Confirm(
                                  assign.userId,
                                  assign.resource,
                                  reply.location.get,
                                  reply.responseTag,
                                  assign.pendingCmdSeqNum,
                                  resolver.toSerializationFormat(replyTo)
                                )
                              }
                            }
                          }

                      case ResourceCmdMessage.SealedValue.Release(release) =>
                        takenResources
                          .ask[ReleaseReply] { replyTo =>
                            resource.Release(
                              release.userId,
                              release.resource,
                              release.location,
                              release.pendingCmdSeqNum,
                              resolver.toSerializationFormat(replyTo)
                            )
                          }
                          .flatMap { reply =>
                            if (reply.responseTag.isUnassignAccepted) {
                              Future.successful(Done)
                            } else {
                              userResourceLinks.askWithStatus[Done] { replyTo =>
                                Confirm(
                                  release.userId,
                                  release.resource,
                                  release.location,
                                  reply.responseTag,
                                  release.pendingCmdSeqNum,
                                  resolver.toSerializationFormat(replyTo)
                                )
                              }
                            }
                          }

                      case ResourceCmdMessage.SealedValue.Reassign(reassign) =>
                        takenResources
                          .ask[ReassignReply] { replyTo =>
                            resource.Reassign(
                              reassign.userId,
                              reassign.resource,
                              reassign.releasedLocation,
                              reassign.pendingCmdSeqNum,
                              resolver.toSerializationFormat(replyTo)
                            )
                          }
                          .flatMap { reply =>
                            if (reply.responseTag.isReassignAccepted)
                              Future.successful(Done)
                            else {
                              userResourceLinks.askWithStatus[Done] { replyTo =>
                                Confirm(
                                  reassign.userId,
                                  reassign.resource,
                                  reply.location.get,
                                  reply.responseTag,
                                  reassign.pendingCmdSeqNum,
                                  resolver.toSerializationFormat(replyTo)
                                )
                              }
                            }
                          }

                      case o =>
                        Future.failed(new Exception(s"Unexpected pending cmd ${o.getClass.getName}"))
                    }
                  case None =>
                    println("1. UnLock " + c.toString + " at " + System.currentTimeMillis())
                    Future.successful(Done)
                }

              case _: DeletedDurableState[_] =>
                Future.successful(Done)
            }
        }
      )
    }

    ShardedDaemonProcess(system).initWithContext(
      name = projectionName,
      initialNumberOfInstances = numberOfSlices,
      behaviorFactory = { daemonContext =>
        val slices = DurableStateSourceProvider
          .sliceRanges(system, R2dbcDurableStateStore.Identifier, daemonContext.totalProcesses)
        val sliceRange = slices(daemonContext.processNumber)
        ProjectionBehavior(projection(sliceRange))
      },
      settings = akka.cluster.sharding.typed.ShardedDaemonProcessSettings(system),
      stopMessage = Some(ProjectionBehavior.Stop),
      shardAllocationStrategy = Some(utils.newLeastShardAllocationStrategy())
    )
  }
}
