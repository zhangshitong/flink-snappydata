package com.rayfay.nrap.flink.snappydata

import java.net.InetAddress
import java.sql.{Connection, ResultSet, SQLException, Statement}

import com.pivotal.gemfirexd.jdbc.ClientAttribute
import scala.collection.JavaConversions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.NonFatal

final class SnappyDataConnectorHelper {

  var useLocatorURL: Boolean = false

  def executeQuery(_conn: Connection, tableName: String,
                   split: SnappyTableInputSplit, query: String, relDestroyVersion: Int): (Statement, ResultSet, String) = {
    val partition = split.asInstanceOf[SnappyTableInputSplit]
    val statement = _conn.createStatement()
    if (!useLocatorURL) {
      val buckets = partition.buckets.mkString(",")
      statement.execute(
        s"call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION('$tableName', '$buckets', $relDestroyVersion)")
    }

    val txId = SnappyDataConnectorHelper.snapshotTxIdForRead.get()
    if (!txId.equals("null")) {
      statement.execute(
        s"call sys.USE_SNAPSHOT_TXID('$txId')")
    }

    val rs = statement.executeQuery(query)
    (statement, rs, txId)
  }

  /**
    * for gemfire xd jdbc driver
    * @param connectionProperties
    * @param split
    * @return
    */
  def getConnection(connectionProperties: ConnectionProperties,
                    split: SnappyTableInputSplit): Connection = {
    val urlsOfNetServerHost = split.asInstanceOf[SnappyTableInputSplit].hostList
    useLocatorURL = SnappyDataConnectorHelper.useLocatorUrl(urlsOfNetServerHost)
    return createConnection(connectionProperties, urlsOfNetServerHost)

  }


  /**
    * for gemfire xd jdbc driver
    * @param connProperties
    * @param hostList
    * @return
    */
  def createConnection(connProperties: ConnectionProperties,
                       hostList: ArrayBuffer[(String, String)]): Connection = {
    val localhost = InetAddress.getLocalHost;
    var index = -1

    val jdbcUrl = if (useLocatorURL) {
      connProperties.url
    } else {
      if (index < 0) index = hostList.indexWhere(_._1.contains(localhost.getHostAddress))
      if (index < 0) index = Random.nextInt(hostList.size)
      hostList(index)._2
    }


    // enable direct ByteBuffers for best performance
    val executorProps = connProperties.executorConnProps
    executorProps.setProperty(ClientAttribute.THRIFT_LOB_DIRECT_BUFFERS, "true")
    // setup pool properties
    val props = NetworkStoreUtil.getAllPoolProperties(jdbcUrl, null,
      connProperties.poolProps.toMap, connProperties.hikariCP, isEmbedded = false)
    try {
      // use jdbcUrl as the key since a unique pool is required for each server
      ConnectionPool.getPoolConnection(jdbcUrl, true, props,
        executorProps, connProperties.hikariCP)
    } catch {
      case sqle: SQLException => if (hostList.size == 1 || useLocatorURL) {
        throw sqle
      } else {
        hostList.remove(index)
        createConnection(connProperties, hostList)
      }
    }
  }

  /**
    * new TransactionId
    * @return
    */
  def refreshNewTransactionId(): String ={
//    val getSnapshotTXId = _conn.prepareStatement("values sys.GET_SNAPSHOT_TXID()")
//    val rs = getSnapshotTXId.executeQuery()
//    rs.next()
//    val txId = rs.getString(1)
//    rs.close()
//    getSnapshotTXId.close()
    val txId = "null";
    SnappyDataConnectorHelper.snapshotTxIdForRead.set(txId)
    txId
  }

  def commitBeforeCloseConnection(_conn: Connection): Unit ={
    val txId = SnappyDataConnectorHelper.snapshotTxIdForRead.get()
    if(!txId.equals("null")){
      val ps = _conn.prepareStatement(s"call sys.COMMIT_SNAPSHOT_TXID(?)")
      ps.setString(1, if (txId == null) "null" else txId)
      ps.executeUpdate()
      ps.close()
    }
    SnappyDataConnectorHelper.snapshotTxIdForRead.set(null)
  }

  def closeConnection(_conn: Connection): Unit ={
    try {
      if(_conn != null){
        _conn.commit()
        _conn.close()
      }
    } catch {
      case NonFatal(e) => {
        e.printStackTrace()
      }
    }
  }


}

object SnappyDataConnectorHelper {
  val JDBC_URL_PREFIX = "snappydata://"

  var snapshotTxIdForRead: ThreadLocal[String] = new ThreadLocal[String]
  var snapshotTxIdForWrite: ThreadLocal[String] = new ThreadLocal[String]

  def getSplits(bucketToServerList: Array[ArrayBuffer[(String, String)]]): Array[SnappyTableInputSplit] = {
    val numPartitions = bucketToServerList.length
    val partitions = new Array[SnappyTableInputSplit](numPartitions)
    for (p <- 0 until numPartitions) {
      var buckets = new mutable.HashSet[Int]()
      buckets += p
      partitions(p) = new SnappyTableInputSplit(p,
        buckets, bucketToServerList(p))
    }
    partitions
  }

  private def useLocatorUrl(hostList: ArrayBuffer[(String, String)]): Boolean = hostList.isEmpty

  def setBucketToServerMappingInfo(bucketToServerMappingStr: String): Array[ArrayBuffer[(String, String)]] = {
    val urlPrefix = "jdbc:" + JDBC_URL_PREFIX
    // no query routing or load-balancing
    val urlSuffix = "/" + ClientAttribute.ROUTE_QUERY + "=false;" +
      ClientAttribute.LOAD_BALANCE + "=false"
    if (bucketToServerMappingStr != null) {
      val arr: Array[String] = bucketToServerMappingStr.split(":")
      val orphanBuckets = ArrayBuffer.empty[Int]
      val noOfBuckets = arr(0).toInt
      // val redundancy = arr(1).toInt
      val allNetUrls = new Array[ArrayBuffer[(String, String)]](noOfBuckets)
      val bucketsServers: String = arr(2)
      val newarr: Array[String] = bucketsServers.split("\\|")
      val availableNetUrls = ArrayBuffer.empty[(String, String)]
      for (x <- newarr) {
        val aBucketInfo: Array[String] = x.split(";")
        val bid: Int = aBucketInfo(0).toInt
        if (!(aBucketInfo(1) == "null")) {
          // get (addr,host,port)
          val hostAddressPort = returnHostPortFromServerString(aBucketInfo(1))
          val netUrls = ArrayBuffer.empty[(String, String)]
          netUrls += hostAddressPort._1 ->
            (urlPrefix + hostAddressPort._2 + "[" + hostAddressPort._3 + "]" + urlSuffix)
          allNetUrls(bid) = netUrls
          for (e <- netUrls) {
            if (!availableNetUrls.contains(e)) {
              availableNetUrls += e
            }
          }
        } else {
          // Save the bucket which does not have a neturl,
          // and later assign available ones to it.
          orphanBuckets += bid
        }
      }
      for (bucket <- orphanBuckets) {
        allNetUrls(bucket) = availableNetUrls
      }
      return allNetUrls
    }
    Array.empty
  }

  def setReplicasToServerMappingInfo(
                                      replicaNodesStr: String): Array[ArrayBuffer[(String, String)]] = {
    val urlPrefix = "jdbc:" + JDBC_URL_PREFIX
    // no query routing or load-balancing
    val urlSuffix = "/" + ClientAttribute.ROUTE_QUERY + "=false;" +
      ClientAttribute.LOAD_BALANCE + "=false"
    val hostInfo = replicaNodesStr.split(";")
    val netUrls = ArrayBuffer.empty[(String, String)]
    for (host <- hostInfo) {
      val hostAddressPort = returnHostPortFromServerString(host)
      netUrls += hostAddressPort._1 ->
        (urlPrefix + hostAddressPort._2 + "[" + hostAddressPort._3 + "]" + urlSuffix)
    }
    Array(netUrls)
  }

  /*
  * The pattern to extract addresses from the result of
  * GET_ALLSERVERS_AND_PREFSERVER2 procedure; format is:
  *
  * host1/addr1[port1]{kind1},host2/addr2[port2]{kind2},...
  */
  private lazy val addrPattern =
    java.util.regex.Pattern.compile("([^,/]*)(/[^,\\[]+)?\\[([\\d]+)\\](\\{[^}]+\\})?")

  private def returnHostPortFromServerString(serverStr: String): (String, String, String) = {
    if (serverStr == null || serverStr.length == 0) {
      return null
    }
    val matcher: java.util.regex.Matcher = addrPattern.matcher(serverStr)
    val matchFound: Boolean = matcher.find
    if (!matchFound) {
      (null, null, null)
    } else {
      val host: String = matcher.group(1)
      // val address: String = matcher.group(2)
      val portStr: String = matcher.group(3)
      // (address, host, portStr)
      (host, host, portStr)
    }
  }



}
