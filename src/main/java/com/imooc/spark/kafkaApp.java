package com.imooc.spark;

public class kafkaApp {
    public static void main(String[] args) {
        // 使用构造方法初始化topic和producer/consumer对象
        // 然后start方法启动对象中的run方法,进行消息的生产和消费
        new kafkaProducer(kafkaProperties.TOPIC).start();
//        new kafkaConsumer(kafkaProperties.TOPIC).start();
    }
}
