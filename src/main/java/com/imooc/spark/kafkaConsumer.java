package com.imooc.spark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class kafkaConsumer extends Thread{
    private String topic;
    private KafkaConsumer<String, String> consumer;

    public kafkaConsumer(String topic) {
        this.topic = topic;

        Properties properties = new Properties();
        properties.put("bootstrap.servers",kafkaProperties.BROKER_LIST);
        properties.put("group.id",kafkaProperties.GROUPID);
        // 开启自动提交offset
        properties.put("enable.auto.commit", "true");
        // 每1000 ms 提交一次 offset，必须开启自动提交offset
        properties.put("auto.commit.interval.ms", "1000");
        // 使用该类将值对象序列化为字节数组
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(properties);
        //订阅主题
        consumer.subscribe(Arrays.asList(topic));
    }

    @Override
    public void run() {
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record: records) {
                System.out.printf("Consumer*** part:%s offset:%s key:%s value:%s\n",record.partition(),record.offset(),record.key(),record.value());
            }
        }
    }
}
