package akka.coordination.lease.psg

import akka.actor.ExtendedActorSystem
import akka.actor.typed.scaladsl.adapter.ClassicActorSystemOps
import akka.cluster.R2dbcSessionProvider
import akka.coordination.lease.LeaseSettings
import akka.coordination.lease.scaladsl.Lease
import akka.persistence.r2dbc.internal.Sql.Interpolation
import akka.projection.r2dbc.scaladsl.R2dbcSession
import akka.util.ConstantFun
import io.r2dbc.spi.Statement

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.Future

object PostgresLease {
  // resources-akka-sbr
  val configPath = "akka.coordination.lease.postgres"
  val SbrPref    = "sbr"
}

/*
  https://doc.akka.io/docs/akka/current/coordination.html
  An akka-coordination lease for Split Brain Resolver
  https://doc.akka.io/libraries/akka-core/current/split-brain-resolver.html#lease
  
  https://doc.akka.io/libraries/akka-core/current/typed/cluster-singleton.html#lease
  https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html#lease
 */
class PostgresLease(system: ExtendedActorSystem, leaseTaken: AtomicBoolean, settings: LeaseSettings)
    extends Lease(settings) {

  import system.dispatcher

  def this(leaseSettings: LeaseSettings, system: ExtendedActorSystem) =
    this(system, new AtomicBoolean(false), leaseSettings)

  system.log.warning(s"★ ★ ★  PostgresLease: $settings ★ ★ ★")

  private val typedSystem     = system.toTyped
  private val sessionProvider = R2dbcSessionProvider(typedSystem, typedSystem.log)

  override def acquire(): Future[Boolean] =
    acquire(ConstantFun.scalaAnyToUnit)

  private def write(session: R2dbcSession, updateStmt: Statement) =
    session.updateOne(updateStmt).map { n =>
      val bool = n == 1
      leaseTaken.set(bool)
      bool
    }

  override def acquire(leaseLostCallback: Option[Throwable] => Unit): Future[Boolean] = {
    val now = System.currentTimeMillis()
    /*
     * IMPORTANT:
     * Setting a lease heartbeat (current value akka.coordination.lease.postgres.heartbeat-interval = 12s)
     * If a node with a lease crashes or is unresponsive the heartbeat-timeout is how long before other nodes can acquire the lease.
     * Without this timeout operator intervention would be needed to release a lease in the case of a node crash. This is the safest option but not practical in all cases.
     *
     * The value should be greater than the max expected JVM pause e.g. garbage collection, otherwise a lease can be acquired by another node and then when the original node becomes
     * responsive again there will be a short time before the original lease owner can take action e.g. shutdown shards or singletons.
     */
    val nextLeaseDeadlineInMillis = now + settings.timeoutSettings.heartbeatTimeout.toMillis + 3_000

    sessionProvider.exec(s"SBR - ${settings.leaseName}: Acquire") { session =>
      system.log.warning(
        s"Acquire lease:${settings.leaseName} by ${settings.ownerName} deadline=$nextLeaseDeadlineInMillis"
      )

      val select =
        session
          .createStatement(sql"SELECT deadline, owner FROM sbr_lease WHERE lease_name= ? FOR UPDATE")
          .bind(0, settings.leaseName)

      session
        .selectOne(select)(row =>
          row.get("deadline", classOf[java.math.BigInteger]) -> row.get("owner", classOf[String])
        )
        .flatMap {
          case Some((deadline, owner)) =>
            val updStmt =
              session
                .createStatement(sql"UPDATE sbr_lease SET deadline = ?, owner = ?  WHERE lease_name= ?")
                .bind(0, nextLeaseDeadlineInMillis)
                .bind(1, settings.ownerName)
                .bind(2, settings.leaseName)

            // A lease has an owner. If the same owner tries to acquire the lease multiple times, it will succeed i.e. leases are reentrant.
            if (owner == settings.leaseName) {
              write(session, updStmt)
            }
            // A lease has another owner.
            else if (deadline.longValue() < now) {
              write(session, updStmt)
            } else {
              system.log.warning(
                s"Fail to acquire lease ${settings.leaseName} by ${settings.ownerName} deadline=$nextLeaseDeadlineInMillis"
              )
              leaseTaken.set(false)
              Future.successful(false)
            }
          case None =>
            system.log.warning(
              s"Insert new lease ${settings.leaseName} by ${settings.ownerName} deadline=$nextLeaseDeadlineInMillis"
            )
            val insert =
              session
                .createStatement(sql"INSERT INTO sbr_lease(lease_name, owner, deadline) VALUES (?,?,?)")
                .bind(0, settings.leaseName)
                .bind(1, settings.ownerName)
                .bind(2, nextLeaseDeadlineInMillis)
            write(session, insert)
        }
    }
  }

  override def release(): Future[Boolean] =
    sessionProvider.exec(s"SBR - ${settings.leaseName}: release") { session =>
      system.log.warning(s"Release lease:${settings.leaseName} by ${settings.ownerName}")
      session
        .updateOne(
          session
            .createStatement(sql"DELETE FROM sbr_lease WHERE lease_name = ? AND owner = ?")
            .bind(0, settings.leaseName)
            .bind(1, settings.ownerName)
        )
        .map { n =>
          leaseTaken.set(false)
          n == 1
        }
    }

  def releaseOnExit(): Future[Boolean] =
    release()

  override def checkLease(): Boolean =
    leaseTaken.get()
}
