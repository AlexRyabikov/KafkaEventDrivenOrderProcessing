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
	consumerGroup := config.GetString("PAYMENT_CONSUMER_GROUP", "payment-service-v1")
	inputTopic := config.GetString("TOPIC_ORDER_CREATED", "order.created")
	outputTopic := config.GetString("TOPIC_PAYMENT_RESULT", "payment.result")
	dlqTopic := config.GetString("TOPIC_PAYMENT_DLQ", "payment.result.dlq")

	reader := kafkaio.NewReader(brokers, consumerGroup, inputTopic)
	defer reader.Close()

	writer := kafkaio.NewWriter(brokers, outputTopic)
	defer writer.Close()

	dlqWriter := kafkaio.NewWriter(brokers, dlqTopic)
	defer dlqWriter.Close()

	log.Println("payment-service started")

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

		status := "PROCESSED"
		reason := ""

		if event.AmountCents > 100_000 {
			status = "FAILED"
			reason = "amount exceeds demo limit"
		}

		out := events.PaymentResultEvent{
			BaseEvent: events.BaseEvent{
				EventID:       uuid.NewString(),
				EventType:     "payment.result",
				EventVersion:  "v1",
				OccurredAtUTC: time.Now().UTC(),
				CorrelationID: event.CorrelationID,
			},
			OrderID:       event.OrderID,
			PaymentStatus: status,
			Reason:        reason,
			SourceService: "payment-service",
		}

		if err := writer.WriteJSON(ctx, event.OrderID, out); err != nil {
			log.Printf("publish payment.result failed: %v", err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit failed: %v", err)
		}
	}
}
