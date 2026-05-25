package akka.cluster

import akka.actor.typed.ActorSystem
import akka.dispatch.ExecutionContexts
import akka.persistence.r2dbc.ConnectionFactoryProvider
import akka.projection.r2dbc.scaladsl.R2dbcSession
import io.r2dbc.postgresql.api.PostgresTransactionDefinition
import io.r2dbc.spi.*
import org.slf4j.Logger
import reactor.core.publisher.Mono

import java.util.UUID
import scala.concurrent.*
import scala.jdk.FutureConverters.*
import scala.util.control.NonFatal

object R2dbcSessionProvider {

  def apply(system: ActorSystem[?], log: Logger): R2dbcSessionProvider =
    new R2dbcSessionProvider(
      ConnectionFactoryProvider(system).connectionFactoryFor("akka.persistence.r2dbc.connection-factory"),
      log
    )(system)
}

final class R2dbcSessionProvider private (connectionFactory: ConnectionFactory, log: Logger)(implicit
  system: ActorSystem[?]
) {

  def exec[T](desc: String)(action: R2dbcSession => Future[T]): Future[T] = {
    // TODO: another ec
    implicit val ec: ExecutionContext = system.executionContext
    acquireCon().flatMap { con =>
      val trxId = UUID.randomUUID()
      val f0    =
        for {
          _ <- toFuture(con.beginTransaction(PostgresTransactionDefinition.from(IsolationLevel.READ_COMMITTED)))
          _ = log.info(s"R2dbc 1.$trxId begin. $desc")
          result <- action(new R2dbcSession(con))
          _      <- toFuture(con.commitTransaction())
          _ = log.info(s"R2dbc 2.$trxId commit")
        } yield result

      f0
        .recoverWith { case NonFatal(ex) =>
          toFuture(con.rollbackTransaction())
            .recover { case NonFatal(_) =>
              log.error(s"R2dbc 2.$trxId rollback", ex)
            }
            .flatMap(_ => Future.failed(ex))
        }
        .andThen { case _ =>
          toFuture(con.close())
        }
    }
  }

  private def acquireCon(): Future[Connection] =
    Mono.from(connectionFactory.create()).toFuture.asScala

  private def toFuture(p: org.reactivestreams.Publisher[Void]): Future[Unit] =
    Mono.from(p).toFuture.asScala.map(_ => ())(ExecutionContexts.parasitic)

}
