package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Properties;

// source code refer: https://github.com/onlybooks/kafka2/blob/main/chapter3/src/main/java/ConsumerSync.java
public class ConsumerSync {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "브로커:9092");
        properties.put("group.id", "y-consumer01"); // 컨슈머 그룹 아이디 정의
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", "latest"); // 컨슈머 오프셋을 찾지 못하는 경우 latest 로 초기화.
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of("y-basic01")); // 구독할 토픽
            // 메시지를 가져오기 위해 카프카에 지속적으로 poll() 호출
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000); // 해당 시간만큼 블럭

                // poll()은 레코드 전체를 리턴. (하나의 메시지만 가져오는 것이 아님)
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Topic: %s, Partition: %s, Offset: %d, Key: %s, Value: %s\n",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }
                consumer.commitSync(); // 현재 배치를 통해 읽은 모든 메시지들을 처리한 후, 추가 메시지를 폴링하기 전 현재의 오프셋을 커밋
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
