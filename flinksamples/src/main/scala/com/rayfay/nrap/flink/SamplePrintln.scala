package com.rayfay.nrap.flink

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Created by STZHANG on 2018/2/7.
  */
object SamplePrintln {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    println("-----SamplePrintln----------")
    println("-----SamplePrintln End----------")

    env.execute("Scala SamplePrintln Example")

  }
}
