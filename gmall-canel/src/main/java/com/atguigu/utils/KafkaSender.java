package com.atguigu.utils;

import com.atguigu.constants.GmallConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaSender {

    private static KafkaProducer<String, String> kafkaProducer;

    //获取producer生产者
    static {

        //broker-list,序列化
        //ProducerConfig
        Properties properties = PropertiesUtil.load("kafka.producer.properties");
        kafkaProducer = new KafkaProducer<String, String>(properties);
    }

    //发送数据（一条数据发送一次）
    public static void send(String topic, String data) {
        kafkaProducer.send(new ProducerRecord<String, String>(topic, data));
    }
}
