package redis

import java.net.InetAddress
import java.time.Duration

import akka.actor.ActorSystem

import scala.concurrent.Future

/**
  * A mutable Redis cluster that uses the CLUSTER NODES command, which broadcasts out to
  * all nodes in the cluster and returns a list of nodes, to connect to new nodes and
  * disconnect from old nodes automatically
  */
class ClusterCommandRedisCluster(configEndpoint: String,
                                 port: Option[Int] = None,
                                 timeout: Option[Duration] = Some(Duration.ofSeconds(30)))
                                (implicit _system: ActorSystem) extends MutableRedisCluster(timeout, Seq()) {

  val ipPortPattern = """(([0-9]{1,3}\.){3}[0-9]{1,3})\:([0-9]+).*"""r("ip", "unused", "port")

  def getNodesFromCluster(): Future[Seq[RedisServer]] = {
    clusterNodes().map(_.map(info => {
      val ipPortPattern(ip, _, port) = info.ip_port
      RedisServer(ip, port.toInt)
    }))
  }

  private def cnameToARecords(cname: String): Seq[String] = InetAddress.getAllByName(cname).map(_.getHostAddress)

  private def seedCluster(): Unit = cnameToARecords(configEndpoint).map(RedisServer(_, port.getOrElse(6379))).foreach(addServer)

  seedCluster()
  asyncRefreshClusterSlots(true)
}