package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/alex/kafka-event-driven-order-processing/pkg/config"
	"github.com/alex/kafka-event-driven-order-processing/pkg/events"
	"github.com/alex/kafka-event-driven-order-processing/pkg/kafkaio"
)

func main() {
	ctx := context.Background()

	brokers := strings.Split(config.GetString("KAFKA_BROKERS", "kafka:9092"), ",")
	consumerGroup := config.GetString("INVENTORY_CONSUMER_GROUP", "inventory-service-v1")
	inputTopic := config.GetString("TOPIC_ORDER_CREATED", "order.created")
	outputTopic := config.GetString("TOPIC_INVENTORY_RESULT", "inventory.result")
	dlqTopic := config.GetString("TOPIC_INVENTORY_DLQ", "inventory.result.dlq")

	reader := kafkaio.NewReader(brokers, consumerGroup, inputTopic)
	defer reader.Close()

	writer := kafkaio.NewWriter(brokers, outputTopic)
	defer writer.Close()

	dlqWriter := kafkaio.NewWriter(brokers, dlqTopic)
	defer dlqWriter.Close()

	log.Println("inventory-service started")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("fetch error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var event events.OrderCreatedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			_ = dlqWriter.WriteJSON(ctx, string(msg.Key), map[string]any{
				"raw":    string(msg.Value),
				"reason": "invalid json",
			})
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		status := "RESERVED"
		reason := ""
		reservedQuantity := event.Quantity

		if event.Quantity > 10 {
			status = "FAILED"
			reason = "not enough stock in demo inventory"
			reservedQuantity = 0
		}

		out := events.InventoryResultEvent{
			BaseEvent: events.BaseEvent{
				EventID:       uuid.NewString(),
				EventType:     "inventory.result",
				EventVersion:  "v1",
				OccurredAtUTC: time.Now().UTC(),
				CorrelationID: event.CorrelationID,
			},
			OrderID:          event.OrderID,
			InventoryStatus:  status,
			ReservedQuantity: reservedQuantity,
			Reason:           reason,
			SourceService:    "inventory-service",
		}

		if err := writer.WriteJSON(ctx, event.OrderID, out); err != nil {
			log.Printf("publish inventory.result failed: %v", err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit failed: %v", err)
		}
	}
}
