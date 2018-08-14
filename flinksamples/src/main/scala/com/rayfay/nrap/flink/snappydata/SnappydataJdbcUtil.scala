package com.rayfay.nrap.flink.snappydata

import java.sql._

import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState

class SnappydataJdbcUtil(val options: JDBCOptions) extends Serializable {
  private val getMetaDataStmtString = "call sys.GET_TABLE_METADATA(?, ?, ?, ?, ?, ?, ?, ?)"
  private var getMetaDataStmt: CallableStatement = _
  private var conn: Connection = _

  

  def partitions(tableName: String) : scala.Array[SnappyTableInputSplit] = {
    runStmtWithExceptionHandling(executeMetaDataStatement(tableName));
    val bucketCount = getMetaDataStmt.getInt(3)
    if (bucketCount > 0) {
       val bucketToServerMappingStr = getMetaDataStmt.getString(6)
       val allNetUrls = SnappyDataConnectorHelper.setBucketToServerMappingInfo(bucketToServerMappingStr)
       SnappyDataConnectorHelper.getSplits(allNetUrls)
    }else{
      val replicaToNodesInfo = getMetaDataStmt.getString(6)
      val allNetUrls = SnappyDataConnectorHelper.setReplicasToServerMappingInfo(replicaToNodesInfo)
      SnappyDataConnectorHelper.getSplits(allNetUrls)
    }
  }

  /**
    * private methods
    * @return
    */
  private def createConnectionFactory(): Connection = {
    val driverClass = options.getJdbcDriverClass
    try {
      val clz = getContextClassLoader.loadClass(driverClass)
      val driver = clz.newInstance.asInstanceOf[Driver]
      DriverManager.registerDriver(driver)
      return driver.connect(options.getJdbcUrl, options.asConnectionProperties)
    } catch {
      case e: ClassNotFoundException =>
        e.printStackTrace()
      case e@(_: SQLException | _: InstantiationException | _: IllegalAccessException) =>
        e.printStackTrace()
    }
    null
  }


  private def initialize(): Unit = {
    conn = createConnectionFactory();
    getMetaDataStmt = conn.prepareCall(getMetaDataStmtString)
  }

  private def runStmtWithExceptionHandling[T](function: => T): T = {
    try {
      if(conn == null || getMetaDataStmt == null){
        initialize()
      }
      function
    } catch {
      case e: SQLException if isConnectionException(e) =>
        conn.close()
        initialize()
        function
    }
  }

  private def isConnectionException(e: SQLException): Boolean = {
    e.getSQLState.startsWith(SQLState.CONNECTIVITY_PREFIX) ||
      e.getSQLState.startsWith(SQLState.LANG_DEAD_STATEMENT)
  }

  private def executeMetaDataStatement(tableName: String): Unit = {
    getMetaDataStmt.setString(1, tableName)
    // Hive table object
    getMetaDataStmt.registerOutParameter(2, java.sql.Types.BLOB)
    // bucket count
    getMetaDataStmt.registerOutParameter(3, java.sql.Types.INTEGER)
    // partitioning columns
    getMetaDataStmt.registerOutParameter(4, java.sql.Types.VARCHAR)
    // index columns
    getMetaDataStmt.registerOutParameter(5, java.sql.Types.VARCHAR)
    // bucket to server or replica to server mapping
    getMetaDataStmt.registerOutParameter(6, java.sql.Types.CLOB)
    // relation destroy version
    getMetaDataStmt.registerOutParameter(7, java.sql.Types.INTEGER)
    // primary key columns
    getMetaDataStmt.registerOutParameter(8, java.sql.Types.VARCHAR)
    getMetaDataStmt.execute
  }

  private def getContextClassLoader = Thread.currentThread.getContextClassLoader

}
