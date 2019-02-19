package com.st;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class Producer {

    private final KafkaProducer<Integer, String> producer;

    public Producer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaProperties.KAFAKA_BROKER_LIST);
        properties.put("key.serizlizer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serizlizer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("client.id", "productDemo");
        this.producer = new KafkaProducer<Integer, String>(properties);
    }

    public void sendMsg() {
        producer.send(new ProducerRecord<Integer, String>(KafkaProperties.TOPIC, 1, "message"), new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                System.out.println("message send to:[" + recordMetadata.partition() + "], offset : [" + recordMetadata.offset() + "]");
            }
        });
    }


    public static void main(String[] args) {
        Producer producer = new Producer();
        producer.sendMsg();
    }
}
