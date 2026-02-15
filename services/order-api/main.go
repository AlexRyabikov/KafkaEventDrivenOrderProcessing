package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/alex/kafka-event-driven-order-processing/pkg/config"
	"github.com/alex/kafka-event-driven-order-processing/pkg/db"
	"github.com/alex/kafka-event-driven-order-processing/pkg/events"
	"github.com/alex/kafka-event-driven-order-processing/pkg/kafkaio"
)

type createOrderRequest struct {
	CustomerEmail string `json:"customer_email"`
	ItemSKU       string `json:"item_sku"`
	Quantity      int    `json:"quantity"`
	AmountCents   int    `json:"amount_cents"`
}

type orderResponse struct {
	ID            string    `json:"id"`
	CustomerEmail string    `json:"customer_email"`
	ItemSKU       string    `json:"item_sku"`
	Quantity      int       `json:"quantity"`
	AmountCents   int       `json:"amount_cents"`
	Status        string    `json:"status"`
	CreatedAtUTC  time.Time `json:"created_at_utc"`
	UpdatedAtUTC  time.Time `json:"updated_at_utc"`
}

func main() {
	ctx := context.Background()

	httpPort := config.GetString("API_PORT", "8080")
	pgDSN := config.GetString("POSTGRES_DSN", "")
	kafkaBrokers := strings.Split(config.GetString("KAFKA_BROKERS", "kafka:9092"), ",")
	topicOrderCreated := config.GetString("TOPIC_ORDER_CREATED", "order.created")

	if pgDSN == "" {
		log.Fatal("POSTGRES_DSN is required")
	}

	pool, err := db.Connect(ctx, pgDSN)
	if err != nil {
		log.Fatalf("db connect failed: %v", err)
	}
	defer pool.Close()

	writer := kafkaio.NewWriter(kafkaBrokers, topicOrderCreated)
	defer writer.Close()

	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, middleware.Recoverer, middleware.Logger)
	r.Use(corsMiddleware)

	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	r.Post("/orders", func(w http.ResponseWriter, req *http.Request) {
		var input createOrderRequest
		if err := json.NewDecoder(req.Body).Decode(&input); err != nil {
			httpError(w, http.StatusBadRequest, "invalid json")
			return
		}

		if input.CustomerEmail == "" || input.ItemSKU == "" || input.Quantity <= 0 || input.AmountCents <= 0 {
			httpError(w, http.StatusBadRequest, "customer_email, item_sku, quantity, amount_cents are required")
			return
		}

		orderID := uuid.NewString()
		now := time.Now().UTC()

		tx, err := pool.Begin(req.Context())
		if err != nil {
			httpError(w, http.StatusInternalServerError, "tx begin failed")
			return
		}
		defer tx.Rollback(req.Context())

		_, err = tx.Exec(req.Context(), `
			INSERT INTO orders (id, customer_email, item_sku, quantity, amount_cents, status, created_at_utc, updated_at_utc)
			VALUES ($1, $2, $3, $4, $5, 'NEW', $6, $6)
		`, orderID, input.CustomerEmail, input.ItemSKU, input.Quantity, input.AmountCents, now)
		if err != nil {
			httpError(w, http.StatusInternalServerError, "insert order failed")
			return
		}

		_, err = tx.Exec(req.Context(), `
			INSERT INTO order_steps (order_id, payment_status, inventory_status, updated_at_utc)
			VALUES ($1, 'PENDING', 'PENDING', $2)
		`, orderID, now)
		if err != nil {
			httpError(w, http.StatusInternalServerError, "insert order_steps failed")
			return
		}

		if err := tx.Commit(req.Context()); err != nil {
			httpError(w, http.StatusInternalServerError, "tx commit failed")
			return
		}

		event := events.OrderCreatedEvent{
			BaseEvent: events.BaseEvent{
				EventID:       uuid.NewString(),
				EventType:     "order.created",
				EventVersion:  "v1",
				OccurredAtUTC: now,
				CorrelationID: orderID,
			},
			OrderID:       orderID,
			CustomerEmail: input.CustomerEmail,
			ItemSKU:       input.ItemSKU,
			Quantity:      input.Quantity,
			AmountCents:   input.AmountCents,
			InitialStatus: "NEW",
			SourceService: "order-api",
		}

		if err := writer.WriteJSON(req.Context(), orderID, event); err != nil {
			httpError(w, http.StatusInternalServerError, "publish order.created failed")
			return
		}

		out := orderResponse{
			ID:            orderID,
			CustomerEmail: input.CustomerEmail,
			ItemSKU:       input.ItemSKU,
			Quantity:      input.Quantity,
			AmountCents:   input.AmountCents,
			Status:        "NEW",
			CreatedAtUTC:  now,
			UpdatedAtUTC:  now,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(out)
	})

	r.Get("/orders", func(w http.ResponseWriter, req *http.Request) {
		rows, err := pool.Query(req.Context(), `
			SELECT id, customer_email, item_sku, quantity, amount_cents, status, created_at_utc, updated_at_utc
			FROM orders
			ORDER BY created_at_utc DESC
			LIMIT 100
		`)
		if err != nil {
			httpError(w, http.StatusInternalServerError, "query orders failed")
			return
		}
		defer rows.Close()

		out := make([]orderResponse, 0, 32)
		for rows.Next() {
			var row orderResponse
			if err := rows.Scan(&row.ID, &row.CustomerEmail, &row.ItemSKU, &row.Quantity, &row.AmountCents, &row.Status, &row.CreatedAtUTC, &row.UpdatedAtUTC); err != nil {
				httpError(w, http.StatusInternalServerError, "scan order failed")
				return
			}
			out = append(out, row)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})

	r.Get("/orders/{id}", func(w http.ResponseWriter, req *http.Request) {
		orderID := chi.URLParam(req, "id")
		var out orderResponse
		err := pool.QueryRow(req.Context(), `
			SELECT id, customer_email, item_sku, quantity, amount_cents, status, created_at_utc, updated_at_utc
			FROM orders WHERE id = $1
		`, orderID).Scan(&out.ID, &out.CustomerEmail, &out.ItemSKU, &out.Quantity, &out.AmountCents, &out.Status, &out.CreatedAtUTC, &out.UpdatedAtUTC)
		if errors.Is(err, pgx.ErrNoRows) {
			httpError(w, http.StatusNotFound, "order not found")
			return
		}
		if err != nil {
			httpError(w, http.StatusInternalServerError, "query order failed")
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})

	server := &http.Server{
		Addr:              ":" + httpPort,
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("order-api listening on :%s", httpPort)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server failed: %v", err)
		}
	}()

	waitForShutdown(server)
}

func waitForShutdown(server *http.Server) {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
}

func httpError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error": message,
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
