package com.spark.demo.kafkaDemo;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.spark.demo.bean.Person;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

public class KafkaDirectDemo {

    private static final SparkConf conf = new SparkConf().setAppName("").setMaster("spark://HOST1:PORT1,HOST2:PORT2");
    private static final JavaStreamingContext CONTEXT = new JavaStreamingContext(conf, Durations.seconds(1000));
    private static final String topicLine = "topic1,topic2";
    private static final String zk = "zookeeper:2181";
    private static final String group = "sparkGroup";

    public void DirectKafkaStream(){
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", zk);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", group);//
        kafkaParams.put("auto.offset.reset", "latest");// 设置只接收最新的数据，不会从头开始读topic的内容
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList(topicLine.split(","));
        JavaInputDStream<ConsumerRecord<Object, Object>> kafkaMessage = KafkaUtils.createDirectStream(CONTEXT,
                LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));

        JavaDStream<String> words = kafkaMessage.mapToPair((recorder) -> (new Tuple2<>(recorder.key().toString(), recorder.value().toString())))
                .map((tuple) -> (tuple._2())).flatMap( (lines) -> ( Arrays.asList(lines.split(" ")).iterator()));

        JavaDStream<String> str = words.transform(new Function2<JavaRDD<String>, Time, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {
                return null;
            }
        });


        JavaPairDStream<String, Integer> counted = words.mapToPair((word) -> (new Tuple2<>(word, 1))).reduceByKey((num1,num2)-> ( num1 + num2));

        counted.print();
        CONTEXT.start();//开启这个流式应用
        try {
            CONTEXT.awaitTermination();//driver阻塞，知道流式应用意外退出
        } catch (InterruptedException e) {
            CONTEXT.stop();//爆出异常，终止程序。
            // 通过调用stop()函数可以优雅退出流式应用
            // 通过将传入的stopSparkContext参数设置为false，可以只停止StreamingContext而不停止SparkContext。
            // 流式应用退出后，不可以通过调用start()函数再次启动
        }
    }

}
