package akka.cluster.ddata

import akka.actor.ExtendedActorSystem
import akka.actor.typed.receptionist.ServiceKey
import akka.cluster.ddata.protobuf.ReplicatedDataSerializer

final class CRDTSerializer(system: ExtendedActorSystem)
    extends ReplicatedDataSerializer(system)
    with akka.cluster.ddata.protobuf.SerializationSupport
    with ProtocDDataSupport {

  override def manifest(obj: AnyRef): String =
    super.manifest(obj)

  override def toBinary(obj: AnyRef): Array[Byte] =
    obj match {
      case reg: akka.cluster.ddata.LWWRegister[_] @unchecked =>
        reg.value match {
          // State from akka.cluster.sharding.DDataShardCoordinator
          case state: akka.cluster.sharding.ShardCoordinator.Internal.State =>
            system.log.warning("Shards online: {} ", state.shards.keySet.size)

          case _ =>
        }
        super.toBinary(obj)

      case orSet: ORSet[_] @unchecked =>
        // system.log.warning("ORSet({})", orSet.elements.mkString(","))
        super.toBinary(orSet)

      case orMultiMap: ORMultiMap[ServiceKey[_], _] @unchecked =>
        // ORMultiMap(204)[ServiceKey[akka.actor.typed.internal.pubsub.TopicImpl$Command](r2dbc-taken-dfn-688),ServiceKey[akka.actor.typed.internal.pubsub.TopicImpl$Command](r2dbc-taken-dfn-953)...]

        /*
        val keys = orMultiMap.underlying.keys.elements
        system.log.warning(
          "ORMultiMap(size={}) {}",
          keys.size,
          /*keys
            .take(3)
            .map { k =>
              k.id + "/" + oRMultiMap.underlying.values.get(k).get.elements.head.asInstanceOf[akka.cluster.typed.internal.receptionist.ClusterReceptionist.Entry].ref.path.toString
            }
            .mkString(",")*/
          keys.take(2).map(_.id).mkString("[", ",", "]")
        )
         */

        super.toBinary(orMultiMap)

      case crdt =>
        // system.log.warning("Other CRDT {}", crdt.getClass.getName)
        super.toBinary(obj)
    }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    super.fromBinary(bytes, manifest)
}
