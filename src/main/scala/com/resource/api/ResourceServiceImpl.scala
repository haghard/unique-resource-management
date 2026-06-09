package com.resource.api

import akka.actor.typed.*
import akka.actor.typed.scaladsl.AskPattern.Askable

import scala.concurrent.*
import com.resource.domain.user.*
import kamon.Kamon

import scala.concurrent.duration.*

final class ResourceServiceImpl(
  userResource: ActorRef[UserCmd]
)(implicit system: ActorSystem[?], timeout: akka.util.Timeout)
    extends ResourceService {

  implicit val sch: Scheduler       = system.scheduler
  implicit val ec: ExecutionContext = system.executionContext

  val logger                             = system.log
  val actorRefResolver: ActorRefResolver = ActorRefResolver(system)

  val retryAfter = system.settings.config.getDuration("retry.after").toMillis.millis

  // val r2dbcDao = new R2dbcDao(system)

  override def assign(request: AssignResourceRequest): Future[ResourceReply] = {
    val requestId = Kamon.currentSpan().trace.id.string
    logger.info(s"[$requestId] assign=${request.userId}")
    Kamon.span("grpc-assign") {
      Kamon
        .currentSpan()
        .tag("userId", request.userId)
        .tag("requestId", requestId)

      userResource
        .askWithStatus[ResourceReply](replyTo =>
          Assign(request.userId, request.resource, actorRefResolver.toSerializationFormat(replyTo))
        )
        .flatMap { reply =>
          if (reply.statusCode.isAborted)
            akka.pattern.after(retryAfter)(
              userResource
                .askWithStatus[ResourceReply](replyTo =>
                  Assign(request.userId, request.resource, actorRefResolver.toSerializationFormat(replyTo))
                )
            )
          else
            Future.successful(reply)
        }
    }
  }

  def release(
    request: com.resource.api.ReleaseResourceRequest
  ): scala.concurrent.Future[com.resource.api.ResourceReply] = {
    val requestId = Kamon.currentSpan().trace.id.string
    logger.info(s"[$requestId] release=${request.userId}")
    Kamon.span("grpc-release") {
      Kamon
        .currentSpan()
        .tag("userId", request.userId)
        .tag("requestId", requestId)

      userResource
        .askWithStatus[ResourceReply](replyTo =>
          Release(request.userId, request.location, actorRefResolver.toSerializationFormat(replyTo))
        )
    }
  }

  override def reassign(request: ReassignResourceRequest): Future[ResourceReply] = {
    val requestId = Kamon.currentSpan().trace.id.string
    logger.info(s"[$requestId] reassign=${request.userId}")
    Kamon.span("grpc-reassign") {
      Kamon
        .currentSpan()
        .tag("userId", request.userId)
        .tag("requestId", requestId)

      userResource
        .askWithStatus[ResourceReply](replyTo =>
          Reassign(
            request.userId,
            request.resource,
            request.location,
            actorRefResolver.toSerializationFormat(replyTo)
          )
        )
        .flatMap { reply =>
          if (reply.statusCode.isAborted)
            akka.pattern.after(retryAfter)(
              userResource
                .askWithStatus[ResourceReply](replyTo =>
                  Reassign(
                    request.userId,
                    request.resource,
                    request.location,
                    actorRefResolver.toSerializationFormat(replyTo)
                  )
                )
            )
          else
            Future.successful(reply)
        }
    }
  }

  override def getResource(request: GetResourceRequest): Future[GetResourceReply] =
    userResource
      .askWithStatus[GetResourceReply](replyTo =>
        GetResource(request.userId, actorRefResolver.toSerializationFormat(replyTo))
      )
}
