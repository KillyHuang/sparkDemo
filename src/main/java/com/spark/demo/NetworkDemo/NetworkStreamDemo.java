package com.spark.demo.NetworkDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class NetworkStreamDemo {

    private static final SparkConf conf = new SparkConf().setAppName("").setMaster("spark://HOST1:PORT1,HOST2:PORT2");
    private static final JavaStreamingContext CONTEXT = new JavaStreamingContext(conf, Durations.seconds(1000));
    private static final String host = "host";
    private static final int port = 0;

    public void NetworkStream(){

        JavaReceiverInputDStream network = CONTEXT.socketTextStream(host,port, StorageLevel.MEMORY_AND_DISK_SER());

        JavaDStream<String> words = network.flatMap(line -> (Arrays.asList(line.toString().split(" "))).iterator());

        JavaPairDStream<String,Integer> result = words.mapToPair(word -> (new Tuple2<>(word,1))).reduceByKey((number1, number2) -> (number1 + number2));

        result.print();

        CONTEXT.start();
        try {
            CONTEXT.awaitTermination();
        }catch (Exception e){
            CONTEXT.stop();
        }
    }


}
