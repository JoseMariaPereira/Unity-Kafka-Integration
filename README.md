# Unity-Kafka-Integration

Kafka Integration for Unity

Contains a Kafka Consumer and a Kafka Producer using dotnet confluent (https://github.com/confluentinc/confluent-kafka-dotnet)

Has a Playermovement that produces to Kafka the movement and the actual player has a Consumer that moves with kafka messages.

To run the programm setup a kafka server in local or remote, add in kafka consumer/producer, your bootstrapserver and your kafka topic without schema
