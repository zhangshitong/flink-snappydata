package com.rayfay.nrap.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.examples.java.wordcount.util.WordCountData

/**
  * com.rayfay.nrap.flink.WindowWordCount
  */
object WindowWordCount {

  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // get input data
    val text =
      if (params.has("input")) {
        // read the text file from given input path
        println(s"readTextFile from  --input ${params.get("input")}")
        env.readTextFile(params.get("input"))
      } else {
        println("Executing WindowWordCount example with default input data set.")
        println("Use --input to specify file input.")
        // get default test text data
        env.fromElements(WordCountData.WORDS: _*)
      }

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    val windowSize = params.getInt("window", 100)
    val slideSize = params.getInt("slide", 10)

    val counts: DataStream[(String, Int)] = text
      // split up the lines in pairs (2-tuple) containing: (word,1)
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(0)
      // create windows of windowSize records slided every slideSize records
      .countWindow(windowSize, slideSize)
      // group by the tuple field "0" and sum up tuple field "1"
      .sum(1)



    // emit result
    if (params.has("output")) {
      counts.writeAsText(params.get("output")).name("outputToTextSink")
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }

    // execute program
    env.execute("WindowWordCount")


  }

}