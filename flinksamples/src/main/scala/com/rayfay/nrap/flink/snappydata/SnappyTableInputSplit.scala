package com.rayfay.nrap.flink.snappydata

import org.apache.flink.core.io.InputSplit

import scala.collection.mutable

class SnappyTableInputSplit(val index: Int, val buckets: mutable.HashSet[Int],
                            val hostList: mutable.ArrayBuffer[(String, String)]) extends InputSplit{
  override def getSplitNumber: Int = {
    index
  }

  def canAcceptHost(host: String): Boolean ={
    !hostList.filter(f=>{f._1.equalsIgnoreCase(host)}).isEmpty
  }

}
