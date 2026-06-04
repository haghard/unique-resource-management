package com.resource.api

import akka.actor.typed.*
import akka.actor.typed.scaladsl.AskPattern.Askable

import scala.concurrent.*
import com.resource.domain.user.*
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.duration.DurationInt

final class ResourceServiceImpl(
  userResource: ActorRef[UserCmd],
  tracer: Tracer
)(implicit system: ActorSystem[_], timeout: akka.util.Timeout)
    extends ResourceService {

  implicit val sch: Scheduler       = system.scheduler
  implicit val ec: ExecutionContext = system.executionContext

  val actorRefResolver: ActorRefResolver = ActorRefResolver(system)

  val retryAfter = 500.millis // TODO: config
  val logger     = system.log

  override def assign(request: AssignResourceRequest): Future[ResourceReply] = {
    val grpcSpan = tracer.spanBuilder(s"assign:${request.userId}").startSpan()
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
        else {
          Future.successful(reply)
        }
      }
      .andThen(_ => grpcSpan.end())
  }

  def release(
    request: com.resource.api.ReleaseResourceRequest
  ): scala.concurrent.Future[com.resource.api.ResourceReply] = {
    val grpcSpan = tracer.spanBuilder(s"grpc.release:${request.userId}").startSpan()
    userResource
      .askWithStatus[ResourceReply] { replyTo =>
        Release(request.userId, request.location, actorRefResolver.toSerializationFormat(replyTo))
      }
      .andThen(_ => grpcSpan.end())
  }

  override def reassign(request: ReassignResourceRequest): Future[ResourceReply] = {
    val grpcSpan = tracer.spanBuilder(s"grpc.reassign:${request.userId}").startSpan()
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
        else {
          Future.successful(reply)
        }
      }
      .andThen(_ => grpcSpan.end())
  }

  override def getResource(request: GetResourceRequest): Future[GetResourceReply] = {
    val grpcSpan = tracer.spanBuilder(s"grpc.getResource:${request.userId}").startSpan()
    userResource
      .askWithStatus[GetResourceReply](replyTo =>
        GetResource(request.userId, actorRefResolver.toSerializationFormat(replyTo))
      )
      .andThen(_ => grpcSpan.end())
  }
}
