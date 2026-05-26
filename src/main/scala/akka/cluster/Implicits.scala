package akka.cluster

object Implicits {

  implicit class MemberOps(val member: Member) extends AnyVal {

    /*
    Member.ageOrdering uses
    if (upNumber == other.upNumber) Member.addressOrdering.compare(address, other.address) < 0 else upNumber < other.upNumber
     */

    // `upNumber` is a monotonically growing sequence number which increases each time new incarnation of the process starts.
    def details: String =
      s"${member.uniqueAddress.address.host.getOrElse("")}:${member.uniqueAddress.longUid}, UpNum(${member.upNumber})"

    def singletonInfo: String =
      s"Singleton ${member.uniqueAddress.address.host.getOrElse("")}:${member.uniqueAddress.longUid} UpNum(${member.upNumber})"
  }
}
