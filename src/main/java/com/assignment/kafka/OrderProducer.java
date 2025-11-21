package com.assignment.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.Random;

public class OrderProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
        Random rand = new Random();
        int idCounter = 1000;

        while (true) {
            idCounter++;
            // Randomized price for assignment [cite: 12]
            float price = 10.0f + rand.nextFloat() * 100.0f;

            Order order = new Order(String.valueOf(idCounter), "Item" + (rand.nextInt(5)+1), price);

            ProducerRecord<String, Order> record = new ProducerRecord<>("orders", order.getOrderId().toString(), order);

            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Sent Order: " + order.getOrderId() + " Price: " + order.getPrice());
                }
            });

            Thread.sleep(1000); // Send one every second
        }
    }
}