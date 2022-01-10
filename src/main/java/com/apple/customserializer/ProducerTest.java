package com.apple.customserializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerTest {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        //kafka集群，broker-list
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数（收不到ack时request.timeout.ms默认30秒就重发消息）
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        //批次大小 16K
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        //等待时间（跟批次大小一起用）
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        //RecordAccumulator缓冲区大小 32M
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());

        Producer<String, Company> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, Company>("serializerplay", Integer.toString(i), Company.of("Apple", Integer.toString(i))));
        }

        producer.close();
    }
}
