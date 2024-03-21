package com.apple.producer;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProducerWithCallbackParallel {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        // Kafka cluster, bootstrap servers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Number of producer threads
        int numThreads = 5;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        for (int i = 0; i < 100; i++) {
            final int messageKey = i;
            executor.submit(() -> {
                Producer<String, String> producer = new KafkaProducer<>(props);
                producer.send(new ProducerRecord<>("first", Integer.toString(messageKey), Integer.toString(messageKey)), (metadata, exception) -> {
                    if (exception == null) {
                        System.out.println("Message sent successfully -> Offset: " + metadata.offset());
                    } else {
                        exception.printStackTrace();
                    }
                    producer.close();
                });
            });
        }

        // Shutdown executor after all tasks are completed
        executor.shutdown();
    }
}
