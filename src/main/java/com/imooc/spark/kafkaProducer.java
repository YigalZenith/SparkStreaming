package com.imooc.spark;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class kafkaProducer extends Thread {
    // 定义私有变量
    private final KafkaProducer<String, String> kafkaProducer;
    private final String topic;

    // 构造方法传入topic,并初始化私有变量
    public kafkaProducer(String topic) {
        this.topic = topic;
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaProperties.BROKER_LIST);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", kafkaProperties.ACKS);
        props.put("retries", kafkaProperties.RETRIES);
        kafkaProducer = new KafkaProducer<>(props);
    }

    @Override
    public void run() {
        for (int i = 1; i <= 11; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "value" + i);
            kafkaProducer.send(record);
            System.out.printf("Producer****** part:%s offset:%s key:%s value:%s ********\n", record.partition(), record.topic(), record.key(), record.value());
            try {
                sleep(5);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
         kafkaProducer.close();
    }
}