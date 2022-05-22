package com.imooc.spark;

public class kafkaProperties {
    public static final String TOPIC = "streaming_offset";
    public static final String BROKER_LIST = "hadoop000:9092";
    public static final String ACKS = "1";
    public static final String RETRIES = "3";
    public static final String GROUPID = "test_group";
}
