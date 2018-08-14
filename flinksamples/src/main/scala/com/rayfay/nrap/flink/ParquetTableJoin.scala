package com.rayfay.nrap.flink

import java.lang
import java.util.Properties

import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.windowing.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSource
import org.apache.flink.table.api.TableSchema
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object ParquetTableJoin {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    val kafkaProps = new Properties();
    kafkaProps.setProperty("bootstrap.servers", "cdh2.rayfay.io:9092,cdh5.rayfay.io:9092,cdh7.rayfay.io:9092")
    kafkaProps.setProperty("group.id", "flink-01")
    kafkaProps.setProperty("auto.offset.reset", "earliest")
    val source = Kafka010JsonTableSource.builder()
      // set Kafka topic
      .forTopic("topicPayInfo")
      // set Kafka consumer properties
      .withKafkaProperties(kafkaProps)
      // set Table schema
      .withSchema(TableSchema.builder()
      .field("APP_ID", Types.STRING)
      .field("APP_TRANS_ID", Types.STRING)
      .field("APP_TRANS_SYNC_NO", Types.STRING).build())
      .build();
      val ds = source.getDataStream(env);//.assignTimestampsAndWatermarks(new PeriodTimestampExtractor(false));
//
      val windowedDS = ds.keyBy("APP_TRANS_ID").timeWindow(Time.seconds(1))

      val leftJoinDs = windowedDS.apply(new NoChangeWindowFunction)
      val otherJoinedDs = windowedDS.process(new CustomerProcessWindowFunction)
           .assignTimestampsAndWatermarks(new PeriodTimestampExtractor(true));

      val joinedDs = otherJoinedDs.join(leftJoinDs).where(new RowIndexKeySelector(0, "leftTable"))
        .equalTo(new RowIndexKeySelector(1, "rightTable")).window(TumblingEventTimeWindows.of(Time.seconds(1)));


//      val joinedDs2 = ds.join(ds).where(new RowIndexKeySelector(1, "leftTable"))
//        .equalTo(new RowIndexKeySelector(1, "rightTable"))
//        .window(TumblingEventTimeWindows.of(Time.seconds(3)));
//      joinedDs2.apply(new InnerJoinTransformer).print()
      val resultDS = joinedDs.apply(new InnerJoinTransformer());
      resultDS.print();


//    ds.join(ds).where().equalTo().window()
//    dataStream.join(otherStream)
//      .where(0).equalTo(1)
//      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//      .apply { ... }

//    ds.join(ds).where(12).equalTo().window().apply(InnerJoinOperatorBase);
    //ds.join(ds).where(1).equalTo().window(); // 必须是window的，必须有where 和EqualTo






//    ds.writeAsText("/stzhang/flink-data/payinfo", org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);
    env.execute("Kafka Window Stream WordCount")

  }


}


class InnerJoinTransformer() extends JoinFunction[Row, Row, Row]{
  override def join(t1: Row, t2: Row): Row = {
    val k : String = t1.getField(1).asInstanceOf[String];
//    println("Thread.key="+k)
    Row.of(t1.getField(0), t2.getField(1), t1.getField(2), t2.getField(0), t2.getField(1))
  }
}

class RowIndexKeySelector(pos: Int, from: String) extends KeySelector[Row, String]{
  override def getKey(in: Row): String = {
    val kValue = in.getField(pos).asInstanceOf[String];
//    println("keyValue="+kValue+" from "+from)
    kValue
  }
}


class CustomerProcessWindowFunction extends ProcessWindowFunction[Row, Row, Tuple, TimeWindow] {
  @Override
  override def process(key: Tuple, context: ProcessWindowFunction[Row, Row, Tuple, TimeWindow]#Context, iterable: lang.Iterable[Row], collector: Collector[Row]): Unit = {
    val k : String = key.getField(0);
    val it = iterable.iterator();
//    println("Thread.currentThread customer process="+Thread.currentThread().getName)
//    Thread.sleep(100)
    while (it.hasNext){
      val r = it.next();
      collector.collect(Row.of(r.getField(1), r.getField(2), new Integer(1)
        ,  new Integer(2), new java.lang.Long(context.window.maxTimestamp)));
    }
  }
}


class NoChangeWindowFunction extends  WindowFunction[Row, Row, Tuple, TimeWindow]{
  override def apply(key: Tuple, w: TimeWindow, iterable: lang.Iterable[Row], collector: Collector[Row]): Unit = {
     val it = iterable.iterator();
     val k : String = key.getField(0);
//     println("getKey="+k)
//     println("Thread.currentThread="+Thread.currentThread().getName)
     while (it.hasNext){
        collector.collect(it.next());
     }

  }
}

class PeriodTimestampExtractor(fromRecord: Boolean) extends AssignerWithPeriodicWatermarks[Row] with Serializable {
  var currentMaxTimestamp: Long = 0L;
  override def extractTimestamp(e: Row, prevElementTimestamp: Long) = {
    val time = if(fromRecord) e.getField(4).asInstanceOf[Long] else System.currentTimeMillis()
    currentMaxTimestamp = Math.max(time, currentMaxTimestamp)
    time
  }

  override def getCurrentWatermark(): Watermark =
    new Watermark(currentMaxTimestamp )
}