package main

import (
	"log"
	"os"
	"time"
)

func main() {
	log.Println("Starting Go Kafka Event Producer...")

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "kafka:9092"
	}

	log.Printf("Configured Kafka Brokers: %s\n", kafkaBrokers)

	// Keep container running in a loop
	for {
		log.Println("Go Kafka Producer is running. Awaiting streaming configuration...")
		time.Sleep(30 * time.Second)
	}
}
