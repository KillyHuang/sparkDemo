package com.spark.demo.kafkaDemo;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
public class kafkaDemo {

    private static final SparkConf conf = new SparkConf().setAppName("").setMaster("spark://HOST1:PORT1,HOST2:PORT2");
    private static final JavaStreamingContext CONTEXT = new JavaStreamingContext(conf, Durations.seconds(1000));
    private static final String topicLine = "topic1,topic2";
    private static final String zk = "zk:9092";
    private static final String group = "group";

    public void kafkaStream(){
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", zk);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", group);//
        kafkaParams.put("auto.offset.reset", "latest");// 知接收最早的数据
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList(topicLine.split(","));
        JavaInputDStream<ConsumerRecord<Object, Object>> kafkaMessage = KafkaUtils.createDirectStream(CONTEXT,
                LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(topics, kafkaParams));

        JavaDStream<String> line = kafkaMessage.mapToPair(new PairFunction<ConsumerRecord<Object, Object>, String, String>() {
            public Tuple2<String, String> call(ConsumerRecord<Object, Object> recorder) throws Exception {
                return new Tuple2<>(recorder.key().toString(), recorder.value().toString());
            }
        }).map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> tuple) throws Exception {
                return tuple._2();
            }
        });

        JavaDStream<String> words = line.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> pairWord = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<>(word, 1);
            }
        });

        JavaPairDStream<String, Integer> counted = pairWord.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer num1, Integer num2) throws Exception {
                return num1 + num2;
            }
        });
        counted.print();
        CONTEXT.start();
        try {
            CONTEXT.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
