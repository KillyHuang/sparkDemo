package com.spark.demo;

import com.spark.demo.kafkaDemo.KafkaDirectDemo;

public class SparkDemo {

    public static void main(String[] args) {
        KafkaDirectDemo kafkaDirectDemo = new KafkaDirectDemo();
        kafkaDirectDemo.DirectKafkaStream();
    }
}
