package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/alex/kafka-event-driven-order-processing/pkg/config"
	"github.com/alex/kafka-event-driven-order-processing/pkg/db"
	"github.com/alex/kafka-event-driven-order-processing/pkg/events"
	"github.com/alex/kafka-event-driven-order-processing/pkg/kafkaio"
)

func main() {
	ctx := context.Background()

	pgDSN := config.GetString("POSTGRES_DSN", "")
	if pgDSN == "" {
		log.Fatal("POSTGRES_DSN is required")
	}

	brokers := strings.Split(config.GetString("KAFKA_BROKERS", "kafka:9092"), ",")
	paymentGroup := config.GetString("ORCHESTRATOR_PAYMENT_GROUP", "orchestrator-payment-v1")
	inventoryGroup := config.GetString("ORCHESTRATOR_INVENTORY_GROUP", "orchestrator-inventory-v1")
	paymentTopic := config.GetString("TOPIC_PAYMENT_RESULT", "payment.result")
	inventoryTopic := config.GetString("TOPIC_INVENTORY_RESULT", "inventory.result")
	statusTopic := config.GetString("TOPIC_ORDER_STATUS_CHANGED", "order.status.changed")

	pool, err := db.Connect(ctx, pgDSN)
	if err != nil {
		log.Fatalf("db connect failed: %v", err)
	}
	defer pool.Close()

	statusWriter := kafkaio.NewWriter(brokers, statusTopic)
	defer statusWriter.Close()

	paymentReader := kafkaio.NewReader(brokers, paymentGroup, paymentTopic)
	defer paymentReader.Close()

	inventoryReader := kafkaio.NewReader(brokers, inventoryGroup, inventoryTopic)
	defer inventoryReader.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		processPaymentLoop(ctx, pool, paymentReader, statusWriter)
	}()

	go func() {
		defer wg.Done()
		processInventoryLoop(ctx, pool, inventoryReader, statusWriter)
	}()

	log.Println("order-orchestrator started")
	wg.Wait()
}

func processPaymentLoop(ctx context.Context, pool *pgxpool.Pool, reader *kafka.Reader, statusWriter *kafkaio.Writer) {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("payment fetch error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var event events.PaymentResultEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		previous, current, err := applyPaymentResult(ctx, pool, event)
		if err != nil {
			log.Printf("apply payment result failed: %v", err)
			continue
		}

		if previous != "" && current != "" && previous != current {
			out := buildStatusChangedEvent(event.OrderID, previous, current)
			if err := statusWriter.WriteJSON(ctx, event.OrderID, out); err != nil {
				log.Printf("publish order.status.changed failed: %v", err)
			}
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("payment commit failed: %v", err)
		}
	}
}

func processInventoryLoop(ctx context.Context, pool *pgxpool.Pool, reader *kafka.Reader, statusWriter *kafkaio.Writer) {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("inventory fetch error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		var event events.InventoryResultEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			_ = reader.CommitMessages(ctx, msg)
			continue
		}

		previous, current, err := applyInventoryResult(ctx, pool, event)
		if err != nil {
			log.Printf("apply inventory result failed: %v", err)
			continue
		}

		if previous != "" && current != "" && previous != current {
			out := buildStatusChangedEvent(event.OrderID, previous, current)
			if err := statusWriter.WriteJSON(ctx, event.OrderID, out); err != nil {
				log.Printf("publish order.status.changed failed: %v", err)
			}
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("inventory commit failed: %v", err)
		}
	}
}

func applyPaymentResult(ctx context.Context, pool *pgxpool.Pool, event events.PaymentResultEvent) (string, string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", "", err
	}
	defer tx.Rollback(ctx)

	processed, err := markProcessed(ctx, tx, "order-orchestrator", event.EventID)
	if err != nil {
		return "", "", err
	}
	if !processed {
		return "", "", tx.Commit(ctx)
	}

	_, err = tx.Exec(ctx, `
		UPDATE order_steps
		SET payment_status = $2, updated_at_utc = now()
		WHERE order_id = $1
	`, event.OrderID, event.PaymentStatus)
	if err != nil {
		return "", "", err
	}

	previous, current, err := finalizeOrderState(ctx, tx, event.OrderID)
	if err != nil {
		return "", "", err
	}

	return previous, current, tx.Commit(ctx)
}

func applyInventoryResult(ctx context.Context, pool *pgxpool.Pool, event events.InventoryResultEvent) (string, string, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return "", "", err
	}
	defer tx.Rollback(ctx)

	processed, err := markProcessed(ctx, tx, "order-orchestrator", event.EventID)
	if err != nil {
		return "", "", err
	}
	if !processed {
		return "", "", tx.Commit(ctx)
	}

	_, err = tx.Exec(ctx, `
		UPDATE order_steps
		SET inventory_status = $2, updated_at_utc = now()
		WHERE order_id = $1
	`, event.OrderID, event.InventoryStatus)
	if err != nil {
		return "", "", err
	}

	previous, current, err := finalizeOrderState(ctx, tx, event.OrderID)
	if err != nil {
		return "", "", err
	}

	return previous, current, tx.Commit(ctx)
}

func markProcessed(ctx context.Context, tx pgx.Tx, serviceName, eventID string) (bool, error) {
	_, err := tx.Exec(ctx, `
		INSERT INTO processed_events (service_name, event_id, processed_at_utc)
		VALUES ($1, $2, now())
	`, serviceName, eventID)
	if err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func finalizeOrderState(ctx context.Context, tx pgx.Tx, orderID string) (string, string, error) {
	var currentOrderStatus string
	var paymentStatus string
	var inventoryStatus string

	err := tx.QueryRow(ctx, `
		SELECT o.status, s.payment_status, s.inventory_status
		FROM orders o
		JOIN order_steps s ON s.order_id = o.id
		WHERE o.id = $1
	`, orderID).Scan(&currentOrderStatus, &paymentStatus, &inventoryStatus)
	if err != nil {
		return "", "", err
	}

	nextStatus := "PROCESSING"
	if paymentStatus == "FAILED" || inventoryStatus == "FAILED" {
		nextStatus = "CANCELLED"
	} else if paymentStatus == "PROCESSED" && inventoryStatus == "RESERVED" {
		nextStatus = "CONFIRMED"
	}

	if nextStatus == currentOrderStatus {
		return currentOrderStatus, currentOrderStatus, nil
	}

	_, err = tx.Exec(ctx, `
		UPDATE orders
		SET status = $2, updated_at_utc = now()
		WHERE id = $1
	`, orderID, nextStatus)
	if err != nil {
		return "", "", err
	}

	return currentOrderStatus, nextStatus, nil
}

func buildStatusChangedEvent(orderID, previous, current string) events.OrderStatusChangedEvent {
	return events.OrderStatusChangedEvent{
		BaseEvent: events.BaseEvent{
			EventID:       uuid.NewString(),
			EventType:     "order.status.changed",
			EventVersion:  "v1",
			OccurredAtUTC: time.Now().UTC(),
			CorrelationID: orderID,
		},
		OrderID:       orderID,
		PreviousState: previous,
		CurrentState:  current,
		SourceService: "order-orchestrator",
	}
}
