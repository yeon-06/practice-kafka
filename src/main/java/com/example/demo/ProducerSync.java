package com.example.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

// source code refer: https://github.com/onlybooks/kafka2/blob/main/chapter3/src/main/java/ProducerSync.java
public class ProducerSync {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "브로커:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); // 내장된 StringSerializer 지정
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
            for (int i = 0; i < 3; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>("y-basic01", "Apache Kafka is a distributed streaming platform - " + i);

                RecordMetadata metadata = producer.send(record)
                        .get(); // get() 메소드를 이용해 카프카의 응답 대기. (메시지 전송 실패 시 예외 발생.)

                System.out.printf("Topic: %s, Partition: %d, Offset: %d, Key: %s, Received Message: %s\n",
                        metadata.topic(), metadata.partition(), metadata.offset(), record.key(), record.value());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
