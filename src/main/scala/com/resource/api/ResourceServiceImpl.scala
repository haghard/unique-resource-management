package com.resource.api

import akka.actor.typed.*
import akka.actor.typed.scaladsl.AskPattern.Askable

import scala.concurrent.*
import com.resource.domain.user.*

import scala.concurrent.duration.DurationInt

final class ResourceServiceImpl(
  userResource: ActorRef[UserCmd]
)(implicit system: ActorSystem[_], timeout: akka.util.Timeout)
    extends ResourceService {

  implicit val sch: Scheduler       = system.scheduler
  implicit val ec: ExecutionContext = system.executionContext

  val actorRefResolver: ActorRefResolver = ActorRefResolver(system)

  val retryAfter = 500.millis // TODO: config

  // val r2dbcDao = new R2dbcDao(system)

  override def assign(request: AssignResourceRequest): Future[ResourceReply] = {
    val s = System.currentTimeMillis()
    println("0. ResourceServiceImpl GRPC at " + s)
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
          val d = System.currentTimeMillis() - s
          println(s"latency:${d}")
          Future.successful(reply)
        }
      }
  }

  def release(
    request: com.resource.api.ReleaseResourceRequest
  ): scala.concurrent.Future[com.resource.api.ResourceReply] = {
    val s = System.currentTimeMillis()
    userResource
      .askWithStatus[ResourceReply] { replyTo =>
        val d = System.currentTimeMillis() - s
        println(s"latency:${d}")
        Release(request.userId, request.location, actorRefResolver.toSerializationFormat(replyTo))
      }
  }

  override def reassign(request: ReassignResourceRequest): Future[ResourceReply] = {
    val s = System.currentTimeMillis()
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
          val d = System.currentTimeMillis() - s
          println(s"latency:${d}")
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
