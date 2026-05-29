package com.resource

import akka.Done
import akka.actor.typed.*
import akka.actor.typed.scaladsl.AskPattern.Askable
import akka.cluster.sharding.typed.*
import akka.cluster.sharding.typed.scaladsl.ShardedDaemonProcess
import akka.cluster.utils
import akka.persistence.query.Offset
import akka.persistence.query.typed.EventEnvelope
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.persistence.r2dbc.query.scaladsl.R2dbcReadJournal
import akka.persistence.typed.PersistenceId
import akka.projection.eventsourced.scaladsl.EventSourcedProvider
import akka.projection.r2dbc.scaladsl.*
import akka.projection.scaladsl.SourceProvider
import akka.projection.*
import com.resource.Implicits.ResourceOps
import com.resource.domain.*
import com.resource.domain.resource.*
import com.resource.domain.resource
import com.resource.domain.user
import com.resource.domain.user.UserCmd

import scala.concurrent.Future

object TakenUniqueResourceProjection {

  val entityName = TakenUniqueResource.TypeKey.name

  private def sourceProvider(
    sliceRange: Range
  )(implicit system: ActorSystem[?]): SourceProvider[Offset, EventEnvelope[ResourceEvent]] =
    EventSourcedProvider.eventsBySlices[ResourceEvent](
      system,
      readJournalPluginId = R2dbcReadJournal.Identifier,
      entityName,
      sliceRange.min,
      sliceRange.max
    )

  def run(
    takenResources: ActorRef[ResourceCmd],
    userResourceLinks: ActorRef[UserCmd],
    numberOfSlices: Int,
    resourceTables: Vector[String],
    askTimeout: akka.util.Timeout
  )(implicit system: ActorSystem[?]): ActorRef[ShardedDaemonProcessCommand] = {
    implicit val to = askTimeout
    val turProjName = entityName + "-proj"

    def projection(sliceRange: Range): Projection[EventEnvelope[ResourceEvent]] = {
      implicit val scheduler         = system.scheduler
      implicit val ec                = system.executionContext
      val minSlice                   = sliceRange.min
      val maxSlice                   = sliceRange.max
      val projectionId               = ProjectionId(turProjName, s"$minSlice-$maxSlice")
      val resolver: ActorRefResolver = ActorRefResolver(system)

      R2dbcProjection.exactlyOnce(
        projectionId,
        settings = None,
        sourceProvider(sliceRange),
        handler = () =>
          (session: R2dbcSession, envelope: EventEnvelope[ResourceEvent]) => {
            // Thread.sleep(500)
            envelope.event.asMessage.sealedValue match {
              case ResourceEventMessage.SealedValue.Assigned(assigned) =>
                assigned.unassignedLocation match {
                  case Some(unassignedLocation) =>
                    println(
                      "5. TakenUniqueResourceProjection: projection get assigned  at " + System.currentTimeMillis()
                    )
                    takenResources.askWithStatus[Done](replyTo =>
                      resource.Unassign(
                        userId = assigned.userId,
                        resource = assigned.resource,
                        assignedLocation = ResourceLocation(
                          PersistenceId.extractEntityId(envelope.persistenceId).toLong,
                          assigned.seqNum
                        ),
                        unassignedLocation = unassignedLocation,
                        pendingCmdSeqNum = assigned.pendingCmdSeqNum,
                        replyTo = resolver.toSerializationFormat(replyTo)
                      )
                    )

                  case None =>
                    userResourceLinks
                      .askWithStatus[Done](replyTo =>
                        user.Confirm(
                          assigned.userId,
                          assigned.resource,
                          ResourceLocation(
                            PersistenceId.extractEntityId(envelope.persistenceId).toLong,
                            assigned.seqNum
                          ),
                          ResponseTag.Assigned,
                          assigned.pendingCmdSeqNum,
                          resolver.toSerializationFormat(replyTo)
                        )
                      )
                      .flatMap { _ =>
                        val resourceTable = tables.resourceTableByUserId(resourceTables, assigned.userId)
                        // val desc          = s"Ev(${envelope.persistenceId}:${envelope.sequenceNr}) [${assigned.userId} / ${ResponseTag.Assigned} / ${assigned.resource.uniqueKey} / ${assigned.pendingCmdSeqNum}]"

                        // sessionProvider.exec(desc) { session =>
                        val stmt =
                          session
                            .createStatement(
                              sql"INSERT INTO $resourceTable (resource_key, resource, user_id, hash_bucket_id, seq_num, modification_time) VALUES (?,?,CAST(? AS UUID),?,?,?)"
                            )
                            .bind(0, assigned.resource.uniqueKey)
                            .bind(1, assigned.resource.toByteArray)
                            .bind(2, assigned.userId)
                            .bind(3, PersistenceId.extractEntityId(envelope.persistenceId).toLong)
                            .bind(4, assigned.seqNum)
                            .bind(5, envelope.timestamp)
                        session.updateOne(stmt).map(_ => Done)
                        // }
                      }
                }

              case ResourceEventMessage.SealedValue.Unassigned(unassigned) =>
                // Thread.sleep(500)
                userResourceLinks
                  .askWithStatus[Done](replyTo =>
                    user.Confirm(
                      unassigned.userId,
                      unassigned.unassignedResource,
                      unassigned.unassignedLocation,
                      ResponseTag.Unassigned,
                      unassigned.pendingCmdSeqNum,
                      resolver.toSerializationFormat(replyTo)
                    )
                  )
                  .flatMap { _ =>
                    val resourceTable = tables.resourceTableByUserId(resourceTables, unassigned.userId)
                    // val desc          = s"Ev(${envelope.persistenceId}:${envelope.sequenceNr}) [${unassigned.userId} / ${ResponseTag.Unassigned} / ${unassigned.unassignedResource.uniqueKey} / ${unassigned.pendingCmdSeqNum}]"
                    // sessionProvider.exec(desc) { session =>
                    val stmt =
                      session
                        .createStatement(
                          sql"DELETE FROM $resourceTable WHERE user_id = CAST(? AS UUID)"
                        ) // WHERE hash_bucket_id = ?, seq_num = ?
                        .bind(0, unassigned.userId)
                    session.updateOne(stmt).map(_ => Done)
                    // }
                  }

              case ResourceEventMessage.SealedValue.Released(reassignedReleased) =>
                userResourceLinks
                  .askWithStatus[Done](replyTo =>
                    user.Confirm(
                      reassignedReleased.userId,
                      reassignedReleased.assignedResource,
                      reassignedReleased.assignedLocation,
                      ResponseTag.Reassigned,
                      reassignedReleased.pendingCmdSeqNum,
                      resolver.toSerializationFormat(replyTo)
                    )
                  )
                  .flatMap { _ =>
                    val resourceTable = tables.resourceTableByUserId(resourceTables, reassignedReleased.userId)
                    // val desc          = s"Ev(${envelope.persistenceId}:${envelope.sequenceNr}) [${reassignedReleased.userId} / ${ResponseTag.Reassigned} / ${reassignedReleased.assignedResource.uniqueKey} / ${reassignedReleased.pendingCmdSeqNum}]"
                    // sessionProvider.exec(desc) { session =>
                    val stmt =
                      session
                        .createStatement(
                          sql"UPDATE $resourceTable SET hash_bucket_id = ?, seq_num = ?, resource = ?, resource_key = ?, modification_time = ? WHERE user_id = CAST(? AS UUID)"
                        )
                        .bind(0, reassignedReleased.assignedLocation.bucketId)
                        .bind(1, reassignedReleased.assignedLocation.seqNum)
                        .bind(2, reassignedReleased.assignedResource.toByteArray)
                        .bind(3, reassignedReleased.assignedResource.uniqueKey)
                        .bind(4, envelope.timestamp)
                        .bind(5, reassignedReleased.userId)
                    session.updateOne(stmt).map(_ => Done)
                    // }
                  }
              case ResourceEventMessage.SealedValue.Empty =>
                Future.successful(Done)
            }
          }
      )(system)
    }

    val ref =
      ShardedDaemonProcess(system).initWithContext(
        name = turProjName,
        initialNumberOfInstances = numberOfSlices,
        behaviorFactory = { daemonContext =>
          val sliceRanges =
            EventSourcedProvider.sliceRanges(system, R2dbcReadJournal.Identifier, daemonContext.totalProcesses)
          ProjectionBehavior(projection(sliceRanges(daemonContext.processNumber)))
        },
        settings = ShardedDaemonProcessSettings(system),
        stopMessage = Some(ProjectionBehavior.Stop),
        shardAllocationStrategy = Some(utils.newLeastShardAllocationStrategy())
      )

    /*
    implicit val scheduler = system.scheduler
    implicit val ec        = system.executionContext
    val d = Random.nextInt(40)
    system.scheduler.scheduleAtFixedRate(d.seconds, d.seconds) { () =>
      ref.ask(akka.cluster.sharding.typed.ChangeNumberOfProcesses(if (Random.nextInt(40) % 2 == 0) 2 else 4, _))
      ()
    }*/

    ref
  }
}
