package com.st;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ProducerWithLoop implements Runnable {

    private final KafkaProducer<Integer, String> producer;

    public ProducerWithLoop() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KafkaProperties.KAFAKA_BROKER_LIST);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
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

    @Override
    public void run() {
        int messageNo = 0;
        while (true) {
            String messageStr = "message-" + messageNo;
            producer.send(new ProducerRecord<Integer, String>(KafkaProperties.TOPIC, messageNo, messageStr), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    System.out.println("message send to:[" + recordMetadata.partition() + "], offset : [" + recordMetadata.offset() + "]");
                }
            });
            ++messageNo;
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws IOException {
        ProducerWithLoop producer = new ProducerWithLoop();
        new Thread(producer).start();
    }

}
