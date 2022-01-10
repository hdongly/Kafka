package com.apple.customserializer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class ConsumerTest {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", CompanyDeserializer.class.getName());

        KafkaConsumer<String, Company> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList("serializerplay"));

        while (true) {
            ConsumerRecords<String, Company> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Company> record : records)
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
        }
    }
}
