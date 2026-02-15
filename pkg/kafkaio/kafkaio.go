package kafkaio

import (
	"context"
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go"
)

type Writer struct {
	writer *kafka.Writer
}

func NewWriter(brokers []string, topic string) *Writer {
	return &Writer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			RequiredAcks: kafka.RequireAll,
			Async:        false,
			Balancer:     &kafka.Hash{},
		},
	}
}

func (w *Writer) WriteJSON(ctx context.Context, key string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	return w.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(key),
		Value: data,
		Time:  time.Now().UTC(),
	})
}

func (w *Writer) Close() error {
	return w.writer.Close()
}

func NewReader(brokers []string, groupID, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		GroupID:     groupID,
		Topic:       topic,
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
		MaxWait:     500 * time.Millisecond,
	})
}
