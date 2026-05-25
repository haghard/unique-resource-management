/*
package akka.cluster

package com.resource

import akka.actor.typed.ActorSystem
import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.persistence.r2dbc.internal.R2dbcExecutor
import akka.persistence.r2dbc.internal.R2dbcExecutor.PublisherOps
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.projection.r2dbc.R2dbcProjectionSettings
import com.resource.api.*
import com.resource.domain.*
import io.r2dbc.spi.Connection
import org.slf4j.LoggerFactory

import scala.concurrent.*

//https://github.com/pgjdbc/r2dbc-postgresql
final class R2dbcDao(system: ActorSystem[?]) {

  val r2dbcSettings     = R2dbcProjectionSettings(system)
  val connectionFactory = ConnectionFactoryProvider(system).connectionFactoryFor(r2dbcSettings.useConnectionFactory)

  /*connectionFactory
    .create()
    .asFuture()
    .map { c =>
      c.beginTransaction(PostgresTransactionDefinition.from(IsolationLevel.REPEATABLE_READ))
    }*/

  val r2dbcExecutor =
    new R2dbcExecutor(
      connectionFactory,
      LoggerFactory.getLogger(classOf[R2dbcExecutor]),
      r2dbcSettings.logDbCallsExceeding
    )(system.executionContext, system)

  implicit val ex: ExecutionContext = ExecutionContext.parasitic

  def selectDefinition(tableName: String) =
    sql"SELECT hash_bucket_id, seq_num, resource FROM $tableName WHERE user_id = CAST(? AS UUID)"

  // r2dbcExecutor.executeDdls(???)

  def getDefinition(ownerId: String, tableName: String): Future[GetResourceLocationReply] =
    r2dbcExecutor
      .selectOne("get")(
        con => {
          //con.beginTransaction()
          con.createStatement(selectDefinition(tableName)).bind(0, ownerId)
        },
        row => {
          val bucketId   = row.get("hash_bucket_id", classOf[java.lang.Long])
          val seqNum     = row.get("seq_num", classOf[java.lang.Long])
          val definition = Resource.parseFrom(row.get("resource", classOf[Array[Byte]]))
          GetResourceLocationReply(Some(ResourceLocation(bucketId, seqNum)), Some(definition))
        }
      )
      .map(_.getOrElse(GetResourceLocationReply(Some(ResourceLocation(-1, -1)), None)))

  private def placeRequest(
    con: Connection,
    in: AssignResourceRequest,
    requestTag: RequestTag
  ): Future[RequestResult] =
    con
      .createStatement(sql"SELECT request, tag FROM pending_requests WHERE user_id = CAST(? AS UUID)")
      .bind(0, in.userId)
      .execute()
      .asFuture()
      .flatMap { pendingRequests =>
        pendingRequests
          .map { pendingRequestsRow =>
            val tag        = pendingRequestsRow.get("tag", classOf[java.lang.Integer])
            val putRequest = ResourceReply.parseFrom(pendingRequestsRow.get("request", classOf[Array[Byte]]))
            if (tag == requestTag.value) {
              if (in == putRequest) RequestResult.Resend(putRequest)
              else RequestResult.ResendInFlightRequestOnConflict(putRequest)
            } else {
              RequestResult.ConcurrentModification
            }
          }
          .asFuture()
          .flatMap { result: RequestResult =>
            if (result ne null)
              Future.successful(result)
            else {
              con
                .createStatement(
                  sql"INSERT INTO pending_requests (user_id, tag, request, ts) VALUES(CAST(? AS UUID),?,?,?)"
                )
                .bind(0, in.userId)
                .bind(1, requestTag.value)
                .bind(2, in.toByteArray)
                .bind(3, System.currentTimeMillis())
                .execute()
                .asFuture()
                .flatMap(_.getRowsUpdated.asFuture().map { n =>
                  if (n == 1) RequestResult.Placed else RequestResult.ConcurrentModification
                })
            }
          }
      }

  def create(in: AllocateResourceRequest, tableName: String): Future[RequestResult] =
    r2dbcExecutor
      .withAutoCommitConnection("create") { con =>
        con
          .createStatement(selectDefinition(tableName))
          .bind(0, in.userId)
          .execute()
          .asFuture()
          .flatMap { definitionResult =>
            val f: Future[RequestResult] =
              definitionResult
                .map { row =>
                  val bucketId   = row.get("hash_bucket_id", classOf[java.lang.Long])
                  val seqNum     = row.get("seq_num", classOf[java.lang.Long])
                  val definition = Resource.parseFrom(row.get("resource", classOf[Array[Byte]]))
                  if (definition == in.resource) {
                    RequestResult.Ok(ResourceLocation(bucketId, seqNum))
                  } else {
                    RequestResult.OwnerReserved(bucketId, seqNum)
                  }
                }
                .asFuture()
                .flatMap { reply: RequestResult =>
                  if (reply == null) placeRequest(con, in, RequestTag.Create)
                  else Future.successful(reply)
                }
            f
          }
      }

  def update(in: AssignResourceRequest, tableName: String): Future[RequestResult] =
    r2dbcExecutor
      .withAutoCommitConnection("update") { con =>
        con
          .createStatement(selectDefinition(tableName))
          .bind(0, in.userId)
          .execute()
          .asFuture()
          .flatMap { definitionResult =>
            definitionResult
              .map { row =>
                val bucketId        = row.get("hash_bucket_id", classOf[java.lang.Long])
                val seqNum          = row.get("seq_num", classOf[java.lang.Long])
                val definition      = Resource.parseFrom(row.get("resource", classOf[Array[Byte]]))
                val currentLocation = ResourceLocation(bucketId, seqNum)
                if (in.location == currentLocation) {
                  if (in.resource == definition) {
                    RequestResult.Ok(currentLocation)
                  } else {
                    RequestResult.Update
                  }
                } else {
                  RequestResult.LocationNotFound
                }
              }
              .asFuture()
              .flatMap { reply: RequestResult =>
                if (reply == null)
                  Future.successful(RequestResult.NotFound)
                else if (reply == RequestResult.Update)
                  placeRequest(con, in, RequestTag.Update)
                else
                  Future.successful(reply)
              }
          }
      }
}

 */
