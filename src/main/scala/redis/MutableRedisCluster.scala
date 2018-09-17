package redis

import akka.actor.ActorSystem
import java.time.Duration

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.concurrent.stm.Ref


/*
 * A Redis cluster that can dynamically add and remove nodes.
 * Refreshes the node listing automatically based on a timeout parameter.
 */
abstract class MutableRedisCluster(timeout: Option[Duration],
                          val initialServers: Seq[RedisServer],
                          override val name: String = "RedisClientPool",
                          password: Option[String] = None)
                         (implicit _system: ActorSystem,
                          redisDispatcher: RedisDispatcher = RedisDispatcher("rediscala.rediscala-client-worker-dispatcher")
                         ) extends RedisCluster {

  def refreshNodeReferences(): Future[Unit] = refreshNodeReferencesHelper(false)

  def getNodesFromCluster(): Future[Seq[RedisServer]]

  override val redisServerConnections: collection.mutable.Map[RedisServer, RedisConnection] = {
    collection.mutable.Map() ++ initialServers.map(makeConnection).toMap[RedisServer, RedisConnection]
  }

  private def makeConnection(server: RedisServer): (RedisServer, RedisConnection) =
    makeRedisConnection(
      server = server.copy(password = password, db = None),
      defaultActive = true
    )

  def redisServers: Seq[RedisServer] = redisServerConnections.synchronized {
    redisServerConnections.keys.toSeq
  }

  def addServer(server: RedisServer) {
    if (!redisServerConnections.contains(server)) {
      redisServerConnections.synchronized {
        if (!redisServerConnections.contains(server)) {
          log.info(s"Adding connection: $server")
          redisServerConnections += makeRedisConnection(server)
        }
      }
    }
  }

  def removeServer(askServer: RedisServer) {
    if (redisServerConnections.contains(askServer)) {
      log.info(s"Removing connection: $askServer")
      redisServerConnections.synchronized {
        redisServerConnections.get(askServer).foreach { redisServerConnection =>
          _system stop redisServerConnection.actor
        }
        redisServerConnections.remove(askServer)
        refreshConnections()
      }
    }
  }

  protected val lockRefreshNodes = Ref(false)

  protected def refreshNodeReferencesHelper(force: Boolean = false): Future[Unit] = {
    if (lockRefreshNodes.single.compareAndSet(false, true)) {
      for {
        refreshedNodes <- getNodesFromCluster()
      } yield {
        val currentNodes = redisServerConnections.toSeq.map(_._1)
        log.debug(s"refreshNodeReferences: $refreshedNodes")
        refreshedNodes match {
          case Nil => log.warning("Refreshed nodes is an empty list - not updating connections")
          case _ =>
            refreshedNodes.foreach(node => {
              if (!currentNodes.contains(node)) {
                addServer(node)
              }
            })
            currentNodes.foreach(node => {
              if (!refreshedNodes.contains(node)) {
                removeServer(node)
              }
            })
        }
        refreshConnections()
        asyncRefreshClusterSlots(force)
      }
      lockRefreshNodes.single.compareAndSet(true, false)
    }
    Future.successful(())
  }

  protected val timeoutMillis: Option[Long] = timeout.map(_.toMillis)

  if(timeout.isDefined) {
    val loopingTimer = timeout.get.getSeconds.seconds
    log.info(s"Registering elasticache node refresh on a loop of $loopingTimer seconds.")
    _system.scheduler.schedule(initialDelay = loopingTimer, interval = loopingTimer)({
      refreshNodeReferences()
    })
  }
}