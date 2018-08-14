package com.rayfay.nrap.flink.snappydata;

import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.core.io.InputSplitAssigner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.ConcurrentMap;

public class SnappydataInputSplitAssigner implements InputSplitAssigner {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnappyDataInputFormat.class);

    private ConcurrentMap<SnappyTableInputSplit, Integer> concurrentHashMap = Maps.newConcurrentMap();
    private SnappyTableInputSplit[] splits = null;
    public SnappydataInputSplitAssigner(SnappyTableInputSplit[] splits){
        this.splits = splits;
    }

    @Override
    public SnappyTableInputSplit getNextInputSplit(String host, int taskId) {
        LOGGER.info("host="+ host);
        if(this.splits != null){
            int size = this.splits.length;
            for (SnappyTableInputSplit split : this.splits) {
                 if(split.canAcceptHost(host)){
                     Integer inte = concurrentHashMap.get(split);
                     if(inte != null && inte.equals(taskId)){
                         LOGGER.info("Found inputSplit "+split.index() + " for task "+ taskId);
                         return split;
                     }
                     if(inte == null){
                         concurrentHashMap.putIfAbsent(split, taskId);
                         LOGGER.info("Found inputSplit "+split.index() + " for task "+ taskId);
                         return split;
                     }
                 }
            }

            //随机产一个
            Random random = new Random();
            int pos = random.nextInt(size);
            int idex = pos;
            SnappyTableInputSplit split = this.splits[idex];
            while(concurrentHashMap.containsKey(split)){
                idex = (idex + 1) % size;
                split = this.splits[idex];
                if(idex == pos){
                    split = null;
                    break;
                }
            }
            if(split != null){
                concurrentHashMap.putIfAbsent(split, taskId);
                LOGGER.info("Found inputSplit "+split.index() + " for task "+ taskId);
                return split;
            }
        }
        LOGGER.info("Not Found inputSplit");
        return null;
    }
}
