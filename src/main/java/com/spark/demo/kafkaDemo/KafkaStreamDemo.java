package com.spark.demo.kafkaDemo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import scala.Tuple2;
import scala.collection.concurrent.CNode;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KafkaStreamDemo {

    private static final SparkConf conf = new SparkConf().setAppName("").setMaster("spark://HOST1:PORT1,HOST2:PORT2");
    private static final JavaSparkContext CONTEXT = new JavaSparkContext(conf);
    private static final String topicLine = "topic1";
    private static final String zk = "zookeeper:2181";
    private static final String group = "sparkGroup";

    public void kafkaStream(){

        OffsetRange[] offsetRange = {
                OffsetRange.create(topicLine,100,0,10000),
                OffsetRange.create(topicLine,10000,0,20000)
        };
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", zk);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", group);//
        kafkaParams.put("auto.offset.reset", "latest");// 设置只接收最新的数据，不会从头开始读topic的内容
        kafkaParams.put("enable.auto.commit", false);

        //CONTEXT.checkpointFile("path:Input_checkPoint");
        JavaRDD<ConsumerRecord<String,String>> kafka = KafkaUtils.createRDD(CONTEXT,kafkaParams,offsetRange, LocationStrategies.PreferConsistent());
        kafka.checkpoint();

        JavaPairRDD<String,Integer> result = kafka.mapToPair((recoder) ->(new Tuple2<>(recoder.key(),recoder.value())))
                .map((tuple) ->(tuple._2()))
                .flatMap((line) ->(Arrays.asList(line.split(" ")).iterator()))
                .mapToPair((word) -> (new Tuple2<>(word,1)))
                .reduceByKey((number1,number2)->(number1+number2));

        result.foreach((tuple)->{
            System.out.println(tuple._1()+"----"+tuple._2());
        });

        result.checkpoint();
        result.saveAsNewAPIHadoopFile("path", Text.class,LongWritable.class, TextOutputFormat.class);

        //CONTEXT.startTime();
        //使用传统的sparkContext进行kafka数据访问
        CONTEXT.stop();
    }
}
