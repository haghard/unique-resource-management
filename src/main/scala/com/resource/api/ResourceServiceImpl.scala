package com.resource.api

import akka.actor.typed.*
import akka.actor.typed.scaladsl.AskPattern.Askable

import scala.concurrent.*
import com.resource.domain.user.*
//import org.HdrHistogram.Histogram

import scala.concurrent.duration.*

final class ResourceServiceImpl(
  userResource: ActorRef[UserCmd]
)(implicit system: ActorSystem[_], timeout: akka.util.Timeout)
    extends ResourceService {

  implicit val sch: Scheduler       = system.scheduler
  implicit val ec: ExecutionContext = system.executionContext

  val logger                             = system.log
  val actorRefResolver: ActorRefResolver = ActorRefResolver(system)

  val retryAfter = system.settings.config.getDuration("retry.after").toMillis.millis

  /*
  val percentiles       = Seq(50.0, 75.0, 90.0, 95.0, 99.0, 99.9)
  val maxHistogramValue = 10L * 1000L // 10 sec max
  var histogram: Histogram = new Histogram(maxHistogramValue, 2)
   */

  // val r2dbcDao = new R2dbcDao(system)

  override def assign(request: AssignResourceRequest): Future[ResourceReply] =
    // val startTs = System.currentTimeMillis()
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
          /*val responseTime = System.currentTimeMillis() - startTs
          histogram.recordValue(responseTime)*/
          Future.successful(reply)
        }
      }

  def release(
    request: com.resource.api.ReleaseResourceRequest
  ): scala.concurrent.Future[com.resource.api.ResourceReply] =
    userResource
      .askWithStatus[ResourceReply](replyTo =>
        Release(request.userId, request.location, actorRefResolver.toSerializationFormat(replyTo))
      )

  override def reassign(request: ReassignResourceRequest): Future[ResourceReply] =
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

  override def getResource(request: GetResourceRequest): Future[GetResourceReply] =
    // histogram.outputPercentileDistribution(System.out, 1.0)

    // akka.persistence.r2dbc.query.refresh-interval = 2s - Max response time=[4447]ms, percentiles [50.0%=48ms; 75.0%=1943ms; 90.0%=1999ms; 95.0%=2007ms; 99.0%=4447ms; 99.9%=4447ms]
    // akka.persistence.r2dbc.query.refresh-interval = 1200ms - Max response time=1223ms, percentiles [50.0%=1087ms; 75.0%=1167ms; 90.0%=1207ms; 95.0%=1215ms; 99.0%=1223ms; 99.9%=1223ms]
    // akka.persistence.r2dbc.query.refresh-interval = 1s - Max response time=1095ms, percentiles [50.0%=891ms; 75.0%=991ms; 90.0%=1007ms; 95.0%=1015ms; 99.0%=1095ms; 99.9%=1095ms]

    /*logger.warn(
      s"Processed ${histogram.getTotalCount()} reqs\n" +
        s"Max response time=${histogram.getMaxValue()}ms, " +
        s"percentiles [${percentiles.map(p => s"$p%=${histogram.getValueAtPercentile(p)}ms").mkString("; ")}]"
    )*/

    userResource
      .askWithStatus[GetResourceReply](replyTo =>
        GetResource(request.userId, actorRefResolver.toSerializationFormat(replyTo))
      )
}
