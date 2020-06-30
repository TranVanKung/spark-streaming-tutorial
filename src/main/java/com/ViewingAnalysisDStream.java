package com;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class ViewingAnalysisDStream {
    public static void main(String[] args) {
        Logger.getLogger("org.apache").setLevel(Level.WARN);
        Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("sparkStreaming");

//        a batch per 2 seconds
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(2));
    }
}
