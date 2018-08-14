package com.rayfay.nrap.flink
import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010

/**
  * Created by STZHANG on 2018/2/8.
  * com.rayfay.nrap.flink.TimeWindowWordCount
  */
object TimeWindowWordCount {
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)
    val producerConfig: Properties = new Properties;
    producerConfig.setProperty("bootstrap.servers", "192.168.106.30:9092,192.168.106.31:9092,192.168.106.32:9092")
    producerConfig.setProperty("zookeeper.connect", "192.168.106.65:2181,192.168.106.66:2181,192.168.106.67:2181")
    val kafkaProducer = new FlinkKafkaProducer010[String]("_worldcount_sample", new SimpleStringSchema, producerConfig)
    kafkaProducer.setWriteTimestampToKafka(true)
    text.addSink(kafkaProducer);

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    counts.writeAsText("/home/stzhang/csv/s1", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);
    counts.print()
    env.execute("Window Stream WordCount")
  }
}
