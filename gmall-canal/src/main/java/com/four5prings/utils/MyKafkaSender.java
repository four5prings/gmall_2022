package com.four5prings.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @ClassName MyKafkaSender
 * @Description
 * @Author Four5prings
 * @Date 2022/6/27 22:42
 * @Version 1.0
 */
public class MyKafkaSender {
    //kafka生产者
    public static KafkaProducer<String, String> kafkaProducer = null;

    //初始化kafka生产者
    public static KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(properties);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return producer;
    }


    public static void send(String topic, String msg) {
        if (kafkaProducer == null) {
            kafkaProducer = createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String, String>(topic,msg));
    }
}
