package akka.coordination.lease.psg

object ps {

  def killStop(pid: Long): Unit = {
    val cmd = List[String]("kill", "-stop", String.valueOf(pid))
    Runtime.getRuntime().exec(cmd.toArray)
  }
}
