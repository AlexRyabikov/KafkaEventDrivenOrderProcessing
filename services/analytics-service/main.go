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
	group := config.GetString("ANALYTICS_CONSUMER_GROUP", "analytics-service-v1")
	topic := config.GetString("TOPIC_ORDER_CREATED", "order.created")

	reader := kafkaio.NewReader(brokers, group, topic)
	defer reader.Close()

	log.Println("analytics-service started")

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("fetch error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var event events.OrderCreatedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		log.Printf("[analytics] order=%s sku=%s qty=%d amount=%d",
			event.OrderID, event.ItemSKU, event.Quantity, event.AmountCents)

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit failed: %v", err)
		}
	}
}
