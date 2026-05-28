package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

// Define schemas matching our data lake and ClickHouse destinations

type Order struct {
	OrderID                  string  `json:"order_id"`
	CustomerID                string  `json:"customer_id"`
	OrderStatus              string  `json:"order_status"`
	OrderPurchaseTimestamp   string  `json:"order_purchase_timestamp"`
	OrderApprovedAt          *string `json:"order_approved_at"`
	OrderDeliveredCarrierDate *string `json:"order_delivered_carrier_date"`
	OrderDeliveredCustomerDate *string `json:"order_delivered_customer_date"`
	OrderEstimatedDeliveryDate string  `json:"order_estimated_delivery_date"`
}

type OrderItem struct {
	OrderID           string  `json:"order_id"`
	OrderItemID       int     `json:"order_item_id"`
	ProductID         string  `json:"product_id"`
	SellerID          string  `json:"seller_id"`
	ShippingLimitDate string  `json:"shipping_limit_date"`
	Price             float64 `json:"price"`
	FreightValue      float64 `json:"freight_value"`
}

type OrderPayment struct {
	OrderID            string  `json:"order_id"`
	PaymentSequential  int     `json:"payment_sequential"`
	PaymentType        string  `json:"payment_type"`
	PaymentInstallments int     `json:"payment_installments"`
	PaymentValue       float64 `json:"payment_value"`
}

type OrderReview struct {
	ReviewID             string  `json:"review_id"`
	OrderID              string  `json:"order_id"`
	ReviewScore          int     `json:"review_score"`
	ReviewCommentTitle   *string `json:"review_comment_title"`
	ReviewCommentMessage *string `json:"review_comment_message"`
	ReviewCreationDate   string  `json:"review_creation_date"`
	ReviewAnswerTimestamp string  `json:"review_answer_timestamp"`
}

// Helpers for type conversions

func cleanString(s string) string {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "\"")
	s = strings.Trim(s, "'")
	return s
}

func stringPtr(s string) *string {
	cleaned := cleanString(s)
	if cleaned == "" || strings.ToLower(cleaned) == "null" {
		return nil
	}
	return &cleaned
}

func parseInt(s string) int {
	cleaned := cleanString(s)
	if cleaned == "" {
		return 0
	}
	val, err := strconv.Atoi(cleaned)
	if err != nil {
		return 0
	}
	return val
}

func parseFloat(s string) float64 {
	cleaned := cleanString(s)
	if cleaned == "" {
		return 0.0
	}
	val, err := strconv.ParseFloat(cleaned, 64)
	if err != nil {
		return 0.0
	}
	return val
}

// Kafka helper to ensure topics are pre-created
func ensureTopic(broker string, topic string) error {
	log.Printf("Checking / creating topic: %s\n", topic)
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return fmt.Errorf("failed to dial broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %w", err)
	}

	controllerAddr := net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port))
	controllerConn, err := kafka.Dial("tcp", controllerAddr)
	if err != nil {
		return fmt.Errorf("failed to dial controller: %w", err)
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		// Ignore topic already exists error
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create topic %s: %w", topic, err)
		}
	}
	log.Printf("Topic %s is ready\n", topic)
	return nil
}

func main() {
	log.Println("Starting Go Kafka Event Producer...")

	// 1. Get configurations
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "/data"
	}

	log.Printf("Kafka Brokers: %s\n", kafkaBrokers)
	log.Printf("Data Directory: %s\n", dataDir)

	// 2. Load and index related datasets (items, payments, reviews) in memory
	log.Println("Loading datasets into memory for fast simulation...")

	itemsMap := make(map[string][]OrderItem)
	paymentsMap := make(map[string][]OrderPayment)
	reviewsMap := make(map[string][]OrderReview)

	// Load items
	itemsFile, err := os.Open(filepath.Join(dataDir, "olist_order_items_dataset.csv"))
	if err != nil {
		log.Fatalf("Error opening order items file: %v", err)
	}
	defer itemsFile.Close()
	csvReader := csv.NewReader(itemsFile)
	if _, err := csvReader.Read(); err != nil {
		log.Fatalf("Error reading header of order items: %v", err)
	}
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading item row: %v", err)
			continue
		}
		if len(record) < 7 {
			continue
		}
		item := OrderItem{
			OrderID:           cleanString(record[0]),
			OrderItemID:       parseInt(record[1]),
			ProductID:         cleanString(record[2]),
			SellerID:          cleanString(record[3]),
			ShippingLimitDate: cleanString(record[4]),
			Price:             parseFloat(record[5]),
			FreightValue:      parseFloat(record[6]),
		}
		itemsMap[item.OrderID] = append(itemsMap[item.OrderID], item)
	}
	log.Printf("Loaded items for %d orders\n", len(itemsMap))

	// Load payments
	paymentsFile, err := os.Open(filepath.Join(dataDir, "olist_order_payments_dataset.csv"))
	if err != nil {
		log.Fatalf("Error opening order payments file: %v", err)
	}
	defer paymentsFile.Close()
	csvReader = csv.NewReader(paymentsFile)
	if _, err := csvReader.Read(); err != nil {
		log.Fatalf("Error reading header of order payments: %v", err)
	}
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading payment row: %v", err)
			continue
		}
		if len(record) < 5 {
			continue
		}
		payment := OrderPayment{
			OrderID:            cleanString(record[0]),
			PaymentSequential:  parseInt(record[1]),
			PaymentType:        cleanString(record[2]),
			PaymentInstallments: parseInt(record[3]),
			PaymentValue:       parseFloat(record[4]),
		}
		paymentsMap[payment.OrderID] = append(paymentsMap[payment.OrderID], payment)
	}
	log.Printf("Loaded payments for %d orders\n", len(paymentsMap))

	// Load reviews
	reviewsFile, err := os.Open(filepath.Join(dataDir, "olist_order_reviews_dataset.csv"))
	if err != nil {
		log.Fatalf("Error opening order reviews file: %v", err)
	}
	defer reviewsFile.Close()
	csvReader = csv.NewReader(reviewsFile)
	if _, err := csvReader.Read(); err != nil {
		log.Fatalf("Error reading header of order reviews: %v", err)
	}
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading review row: %v", err)
			continue
		}
		if len(record) < 7 {
			continue
		}
		review := OrderReview{
			ReviewID:             cleanString(record[0]),
			OrderID:              cleanString(record[1]),
			ReviewScore:          parseInt(record[2]),
			ReviewCommentTitle:   stringPtr(record[3]),
			ReviewCommentMessage: stringPtr(record[4]),
			ReviewCreationDate:   cleanString(record[5]),
			ReviewAnswerTimestamp: cleanString(record[6]),
		}
		reviewsMap[review.OrderID] = append(reviewsMap[review.OrderID], review)
	}
	log.Printf("Loaded reviews for %d orders\n", len(reviewsMap))

	// 3. Load and sort orders
	ordersFile, err := os.Open(filepath.Join(dataDir, "olist_orders_dataset.csv"))
	if err != nil {
		log.Fatalf("Error opening orders file: %v", err)
	}
	defer ordersFile.Close()
	csvReader = csv.NewReader(ordersFile)
	if _, err := csvReader.Read(); err != nil {
		log.Fatalf("Error reading header of orders: %v", err)
	}
	var orders []Order
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading order row: %v", err)
			continue
		}
		if len(record) < 8 {
			continue
		}
		order := Order{
			OrderID:                  cleanString(record[0]),
			CustomerID:                cleanString(record[1]),
			OrderStatus:              cleanString(record[2]),
			OrderPurchaseTimestamp:   cleanString(record[3]),
			OrderApprovedAt:          stringPtr(record[4]),
			OrderDeliveredCarrierDate: stringPtr(record[5]),
			OrderDeliveredCustomerDate: stringPtr(record[6]),
			OrderEstimatedDeliveryDate: cleanString(record[7]),
		}
		orders = append(orders, order)
	}
	log.Printf("Loaded %d orders. Sorting chronologically...\n", len(orders))

	// Sort chronologically (using OrderPurchaseTimestamp format YYYY-MM-DD HH:MM:SS)
	sort.Slice(orders, func(i, j int) bool {
		return orders[i].OrderPurchaseTimestamp < orders[j].OrderPurchaseTimestamp
	})
	log.Println("Sorted orders successfully.")

	// 4. Initialize Kafka topics
	brokers := strings.Split(kafkaBrokers, ",")
	topics := []string{"orders", "order_items", "order_payments", "order_reviews"}

	// Retry connecting/checking Kafka
	for i := 0; i < 20; i++ {
		err = nil
		for _, t := range topics {
			if errTopic := ensureTopic(brokers[0], t); errTopic != nil {
				err = errTopic
				break
			}
		}
		if err == nil {
			break
		}
		log.Printf("Waiting for Kafka brokers to be ready (attempt %d/20)... error: %v\n", i+1, err)
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		log.Fatalf("Could not verify Kafka topics: %v", err)
	}

		// 5. Create Kafka Writers
	ordersWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "orders",
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}
	defer ordersWriter.Close()

	itemsWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "order_items",
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}
	defer itemsWriter.Close()

	paymentsWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "order_payments",
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}
	defer paymentsWriter.Close()

	reviewsWriter := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        "order_reviews",
		Balancer:     &kafka.LeastBytes{},
		BatchTimeout: 10 * time.Millisecond,
	}
	defer reviewsWriter.Close()

	// 6. Start Streaming Simulation
	// 20000 orders/hour = 20000 orders / 3600 seconds = 5.55 orders/second = 1 order event package every ~180ms.
	intervalMs := 3600 * 1000 / 20000
	delay := time.Duration(intervalMs) * time.Millisecond
	log.Printf("Starting stream. Speed: 20000 orders/hour. Event interval: %v\n", delay)

	ctx := context.Background()
	for {
		for idx, order := range orders {
			startTime := time.Now()

			// Send Order
			orderJSON, err := json.Marshal(order)
			if err != nil {
				log.Printf("Failed to marshal order: %v\n", err)
				continue
			}
			err = ordersWriter.WriteMessages(ctx, kafka.Message{
				Key:   []byte(order.OrderID),
				Value: orderJSON,
			})
			if err != nil {
				log.Printf("Failed to write order: %v\n", err)
			}

			// Send corresponding items
			if items, ok := itemsMap[order.OrderID]; ok {
				for _, item := range items {
					itemJSON, _ := json.Marshal(item)
					_ = itemsWriter.WriteMessages(ctx, kafka.Message{
						Key:   []byte(item.OrderID),
						Value: itemJSON,
					})
				}
			}

			// Send corresponding payments
			if payments, ok := paymentsMap[order.OrderID]; ok {
				for _, payment := range payments {
					paymentJSON, _ := json.Marshal(payment)
					_ = paymentsWriter.WriteMessages(ctx, kafka.Message{
						Key:   []byte(payment.OrderID),
						Value: paymentJSON,
					})
				}
			}

			// Send corresponding reviews
			if reviews, ok := reviewsMap[order.OrderID]; ok {
				for _, review := range reviews {
					reviewJSON, _ := json.Marshal(review)
					_ = reviewsWriter.WriteMessages(ctx, kafka.Message{
						Key:   []byte(review.OrderID),
						Value: reviewJSON,
					})
				}
			}

			if idx > 0 && idx%1000 == 0 {
				log.Printf("Stream status: processed %d/%d orders. Current purchase timestamp: %s\n", idx, len(orders), order.OrderPurchaseTimestamp)
			}

			// Adjust sleep time to keep stable pacing
			elapsed := time.Since(startTime)
			if elapsed < delay {
				time.Sleep(delay - elapsed)
			}
		}

		log.Println("Reached end of historical dataset. Wrapping around to start streaming from the beginning...")
	}
}
