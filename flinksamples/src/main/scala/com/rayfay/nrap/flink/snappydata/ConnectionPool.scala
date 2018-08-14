package com.rayfay.nrap.flink.snappydata

import java.sql.Connection
import java.util.Properties

import com.pivotal.gemfirexd.Attribute
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import javax.sql.DataSource

import scala.collection.mutable

object ConnectionPool {
  private[this] type PoolKey = (Properties, Properties, Boolean)

  private[this] val idToPoolMap = new java.util.concurrent.ConcurrentHashMap[
    (String, String), (DataSource, PoolKey)](8, 0.75f, 1)

  private[this] val pools = mutable.Map[PoolKey, (DataSource, mutable.Set[String])]()

  def getPoolDataSource(id: String, props: Map[String, String],
                        connectionProps: Properties, hikariCP: Boolean): DataSource = {
    // fast lock-free path first (the usual case)
    val username = props.getOrElse(Attribute.USERNAME_ALT_ATTR.toLowerCase, "")
    val lookupKey = id -> username
    val dsKey = idToPoolMap.get(lookupKey)
    if (dsKey != null) {
      dsKey._1
    } else pools.synchronized {
      // double check after the global lock
      val dsKey = idToPoolMap.get(lookupKey)
      if (dsKey != null) {
        dsKey._1
      } else {
        // search if there is already an existing pool with same properties
        val poolProps = new Properties()
        for ((k, v) <- props) poolProps.setProperty(k, v)
        val poolKey: PoolKey = (poolProps, connectionProps, hikariCP)
        pools.get(poolKey) match {
          case Some((newDS, ids)) =>
            ids += id
            val err = idToPoolMap.putIfAbsent(lookupKey, (newDS, poolKey))
            require(err == null, s"unexpected existing pool for $id: $err")
            newDS
          case None =>
            // create new pool
            val newDS: DataSource = {
              val hconf = new HikariConfig(poolProps)
              if (connectionProps != null) {
                hconf.setDataSourceProperties(connectionProps)
              }
              new HikariDataSource(hconf)
            }

            pools(poolKey) = (newDS, mutable.Set(id))
            val err = idToPoolMap.putIfAbsent(lookupKey, (newDS, poolKey))
            require(err == null, s"unexpected existing pool for $id: $err")
            newDS
        }
      }
    }
  }

  def getPoolConnection(id: String, isGemfireXdDialect: Boolean = true,
                        poolProps: Map[String, String], connProps: Properties,
                        hikariCP: Boolean): Connection = {
    val ds = getPoolDataSource(id, poolProps, connProps, hikariCP)
    val conn = ds.getConnection
    if(isGemfireXdDialect){
      conn.setTransactionIsolation(Connection.TRANSACTION_NONE)
    }
    conn
  }



}
