package com.spark.demo.flumeDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

public class flumeDemo {

    private static final SparkConf conf = new SparkConf().setAppName("").setMaster("spark://HOST1:PORT1,HOST2:PORT2");
    private static final JavaStreamingContext CONTEXT = new JavaStreamingContext(conf, Durations.seconds(1000));
    private static final String host = "HOST";
    private static final int port = 0;

    public void flumeStream(){

        JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = FlumeUtils.createStream(CONTEXT,host,port);
        flumeStream.count();

        flumeStream.count().map((along) ->("Received "+along+ "Flume Event.")).print();

        CONTEXT.start();
        try {
            CONTEXT.awaitTermination();
        }catch (Exception e){
            CONTEXT.stop();
        }
    }
}
