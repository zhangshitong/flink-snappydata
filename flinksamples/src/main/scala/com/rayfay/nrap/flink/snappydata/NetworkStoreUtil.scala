package com.rayfay.nrap.flink.snappydata

object NetworkStoreUtil {
  private def defaultMaxExternalPoolSize: String =
    String.valueOf(math.max(256, Runtime.getRuntime.availableProcessors() * 8))

  private def defaultMaxEmbeddedPoolSize: String =
    String.valueOf(math.max(256, Runtime.getRuntime.availableProcessors() * 16))


  private def addProperty(props: Map[String, String], key: String,
                          value: String): Map[String, String] = {
    if (props.contains(key)) props
    else props + (key -> value)
  }

  def getAllPoolProperties(url: String, driver: String,
                           poolProps: Map[String, String], hikariCP: Boolean,
                           isEmbedded: Boolean): Map[String, String] = {
    // setup default pool properties
    var props = poolProps
    if (driver != null && !driver.isEmpty) {
      props = addProperty(props, "driverClassName", driver)
    }
    val defaultMaxPoolSize = if (isEmbedded) defaultMaxEmbeddedPoolSize
    else defaultMaxExternalPoolSize
    if (hikariCP) {
      props = props + ("jdbcUrl" -> url)
      props = addProperty(props, "maximumPoolSize", defaultMaxPoolSize)
      props = addProperty(props, "minimumIdle", "10")
      props = addProperty(props, "idleTimeout", "120000")
    } else {
      props = props + ("url" -> url)
      props = addProperty(props, "maxActive", defaultMaxPoolSize)
      props = addProperty(props, "maxIdle", defaultMaxPoolSize)
      props = addProperty(props, "initialSize", "4")
    }
    props
  }
}
