package com.assignment.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer; // Needed for DLQ Producer
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {

    // Aggregation Variables
    private static double totalSum = 0;
    private static int count = 0;

    public static void main(String[] args) {
        // 1. Consumer Config
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-analytics-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("specific.avro.reader", "true"); // IMPORTANT: Casts to Order object

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("orders"));

        // 2. DLQ Producer Config (To send failed messages)
        Properties dlqProps = new Properties();
        dlqProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        dlqProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        dlqProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        dlqProps.put("schema.registry.url", "http://localhost:8081");
        KafkaProducer<String, Order> dlqProducer = new KafkaProducer<>(dlqProps);

        System.out.println("Consumer Started...");

        while (true) {
            ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Order> record : records) {
                boolean success = false;
                int retryCount = 0;
                int maxRetries = 3;

                // RETRY LOGIC LOOP
                while (!success && retryCount < maxRetries) {
                    try {
                        processOrder(record.value());
                        success = true;
                    } catch (Exception e) {
                        retryCount++;
                        System.err.println("Processing failed. Retrying " + retryCount + "/" + maxRetries);
                        try { Thread.sleep(1000); } catch (InterruptedException ignored) {}
                    }
                }

                // DEAD LETTER QUEUE LOGIC
                if (!success) {
                    System.err.println("Message permanently failed. Moving to DLQ: " + record.key());
                    dlqProducer.send(new ProducerRecord<>("orders-dlq", record.key(), record.value()));
                }
            }
        }
    }

    private static void processOrder(Order order) {
        // Simulation: Fail if price is greater than 100 to test DLQ
        if (order.getPrice() > 100) {
            throw new RuntimeException("Simulated Processing Error");
        }

        // Aggregation Logic
        totalSum += order.getPrice();
        count++;
        double average = totalSum / count;

        System.out.printf("Order: %s | Price: %.2f | Running Avg: %.2f%n",
                order.getOrderId(), order.getPrice(), average);
    }
}