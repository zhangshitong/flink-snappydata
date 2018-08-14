package com.rayfay.nrap.flink.snappydata

import java.util.Properties
case class ConnectionProperties(url: String, driver: String,
                                poolProps: java.util.Map[String, String],
                                connProps: Properties, executorConnProps: Properties, hikariCP: Boolean = true)

