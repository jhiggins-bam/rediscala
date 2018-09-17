package redis

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem

import scala.concurrent.Await
import scala.concurrent.duration.Duration


/*
 * An immutable Redis cluster implementation. Only the servers that are passed to the constructor will be used.
 */
class ImmutableRedisCluster(val redisServers: Seq[RedisServer],
                            override val name: String = "RedisClientPool",
                            password: Option[String] = None)
                           (implicit _system: ActorSystem,
                            redisDispatcher: RedisDispatcher = RedisDispatcher("rediscala.rediscala-client-worker-dispatcher")
                           ) extends RedisCluster {
  override val redisServerConnections = {
    redisServers.map { server =>
      makeRedisConnection(server, defaultActive = true)
    } toMap
  }

  refreshConnections()
  Await.result(asyncRefreshClusterSlots(force=true), Duration(10,TimeUnit.SECONDS))
}
