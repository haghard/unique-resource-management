package com.resource

import akka.actor.ActorSystem

import java.nio.file.{Files, Paths}
import scala.concurrent.Future

// Enabled in application.conf

/** https://doc.akka.io/docs/akka-management/current/healthchecks.html
  *
  * Sometimes, while initializing, applications have to meet certain conditions before they become ready to serve
  * traffic. These conditions include ensuring that the depending service is ready, or acknowledging that a large
  * dataset needs to be loaded, etc. In such cases, we use Readiness Probes and wait for a certain condition to occur.
  * Only then, the application can serve traffic.
  *
  * When defining both Readiness and Liveness Probes, it is recommended to allow enough time for the Readiness Probe to
  * possibly fail a few times before a pass, and only then check the Liveness Probe. If Readiness and Liveness Probes
  * overlap there may be a risk that the container never reaches ready state, being stuck in an infinite re-create -
  * fail loop.
  *
  * The kubelet can optionally perform and react to probe checks on running containers:
  *
  * LivenessProbe: Indicates whether the container is running. If the liveness probe fails, the kubelet kills the
  * container, and the container is subjected to its restart policy. If a Container does not provide a liveness probe,
  * the default state is Success.
  *
  * ReadinessProbe: Indicates whether the container is ready to respond to requests. If the readiness probe fails, the
  * endpoints controller removes the Pod's IP address from the endpoints of all Services that match the Pod. The default
  * state of readiness before the initial delay is Failure. If a Container does not provide a readiness probe, the
  * default state is Success.
  */
class ShutdownHookReadinessCheck(system: ActorSystem) extends (() => Future[Boolean]) {
  private val shutdownIndicatorFilePath = Paths.get("/tmp/shutdown")

  // During rolling deployment we want to wait until Readiness Probe to fail so that the pod could be removed from rotation by Ingrees and only then proceed termination.
  private def processReadiness(): Boolean =
    if (Files.exists(shutdownIndicatorFilePath)) {
      // Need to wait for readiness check to fail because we want ingress to take this process out of rotation so that no http traffic is route here
      val msg = s"★ ★ ★  CoordinatedShutdown.0 [Readiness check failed. Signal shutdown to pod (SIGTERM)] ★ ★ ★"
      system.log.warning(msg)
      false
    } else {
      true
    }

  override def apply(): Future[Boolean] =
    Future.successful(processReadiness())
}
