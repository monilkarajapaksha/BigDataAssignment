# Kafka Order Processing System

A robust, real-time stream processing application built with **Java**, **Apache Kafka**, and **Avro Serialization**. This system simulates an e-commerce order stream, calculates real-time pricing analytics, and handles system failures gracefully using Retry patterns and a Dead Letter Queue (DLQ).

## Project Overview

This project demonstrates a "Big Data" microservices architecture. It consists of a Producer that generates synthetic order data and a Consumer that processes these orders to calculate a running average of sales prices.

**Key Features:**

  * **Stream Processing:** Real-time aggregation of order prices.
  * **Data Serialization:** Uses **Apache Avro** and Confluent Schema Registry for efficient, strongly-typed data exchange.
  * **Fault Tolerance:**
      * [cite\_start]**Retry Logic:** Automatically retries processing upon temporary failures[cite: 6].
      * [cite\_start]**Dead Letter Queue (DLQ):** Redirects permanently failed messages to a separate topic (`orders-dlq`) for later inspection.

-----

## Architecture

The system follows a decoupled Event-Driven Architecture.

### Data Flow Diagram

![System Architecture Diagram](images/architecture.png)

### Component Breakdown

1.  **Infrastructure (Docker):**

      * **Zookeeper:** Manages the cluster state.
      * **Kafka Broker:** The central log storage for events.
      * **Schema Registry:** Stores the Avro schema (`order.avsc`) to ensure data contract validity between Producer and Consumer.

2.  **The Producer (`OrderProducer.java`):**

      * [cite\_start]Generates random orders with fields: `orderId`, `product`, `price`.
      * Serializes data into binary Avro format.
      * Publishes to the `orders` topic.

3.  **The Consumer (`OrderConsumer.java`):**

      * Listens to the `orders` topic.
      * Deserializes Avro back to the Java `Order` object.
      * Maintains a stateful `running average` calculation.
      * Implements a `try-catch` block for retries and routing to the DLQ.

## Getting Started

### 1\. Prerequisites

  * Docker Desktop installed and running.
  * Java JDK 11 or higher.
  * Maven installed (or use IntelliJ's bundled Maven).

### 2\. Start Infrastructure

The project requires Zookeeper, Kafka, and Schema Registry to be running.

```bash
docker-compose up -d
```
### 3\. Build the Project

This step generates the Java sources from the Avro schema file.

```bash
mvn clean install
```

### 4\. Run the Application

**Step A: Start the Consumer**
Run the `OrderConsumer` class. It will start up and wait for data.

> *Console Output:* `Consumer Started...`

**Step B: Start the Producer**
Run the `OrderProducer` class.

> *Console Output:* `Sent Order: 1001 Price: 55.0`

**Step C: Observe Analytics**
Check the Consumer console to see the real-time aggregation:

> *Console Output:* `Order: 1001 | Price: 55.00 | Running Avg: 55.00`
