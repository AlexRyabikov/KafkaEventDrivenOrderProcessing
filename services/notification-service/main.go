package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/alex/kafka-event-driven-order-processing/pkg/config"
	"github.com/alex/kafka-event-driven-order-processing/pkg/events"
	"github.com/alex/kafka-event-driven-order-processing/pkg/kafkaio"
)

func main() {
	ctx := context.Background()

	brokers := strings.Split(config.GetString("KAFKA_BROKERS", "kafka:9092"), ",")
	group := config.GetString("NOTIFICATION_CONSUMER_GROUP", "notification-service-v1")
	topic := config.GetString("TOPIC_ORDER_STATUS_CHANGED", "order.status.changed")

	reader := kafkaio.NewReader(brokers, group, topic)
	defer reader.Close()

	log.Println("notification-service started")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("fetch error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var event events.OrderStatusChangedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		log.Printf("[notify] order=%s old=%s new=%s correlation_id=%s",
			event.OrderID, event.PreviousState, event.CurrentState, event.CorrelationID)

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit failed: %v", err)
		}
	}
}
