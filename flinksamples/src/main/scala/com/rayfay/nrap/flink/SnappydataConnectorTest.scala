package com.rayfay.nrap.flink
import org.apache.flink.api.scala._
import com.rayfay.nrap.flink.snappydata.SnappyDataInputFormat
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

object SnappydataConnectorTest {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val parameters = new Configuration();
    parameters.setString(SnappyDataInputFormat.jdbcUrlKey, "jdbc:snappydata://192.168.106.91:1527");
    parameters.setString(SnappyDataInputFormat.jdbcTableKey, "APP.PARTSUPP");
    val tableRows: DataSet[Row]  = env.createInput(new SnappyDataInputFormat())
        .withParameters(parameters);
    val partitionDataRows = tableRows.mapPartition((it: Iterator[Row], collector: Collector[Row])=>{
       while (it.hasNext){
         collector.collect(it.next());
       }
    })
    partitionDataRows.print();
    println(partitionDataRows.count());

//    env.execute("Snappydata Dataset reading")
  }
}
