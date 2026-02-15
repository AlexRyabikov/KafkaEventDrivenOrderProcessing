package events

import "time"

type BaseEvent struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	EventVersion  string    `json:"event_version"`
	OccurredAtUTC time.Time `json:"occurred_at_utc"`
	CorrelationID string    `json:"correlation_id"`
}

type OrderCreatedEvent struct {
	BaseEvent
	OrderID       string `json:"order_id"`
	CustomerEmail string `json:"customer_email"`
	ItemSKU       string `json:"item_sku"`
	Quantity      int    `json:"quantity"`
	AmountCents   int    `json:"amount_cents"`
	InitialStatus string `json:"initial_status"`
	SourceService string `json:"source_service"`
}

type PaymentResultEvent struct {
	BaseEvent
	OrderID       string `json:"order_id"`
	PaymentStatus string `json:"payment_status"`
	Reason        string `json:"reason,omitempty"`
	SourceService string `json:"source_service"`
}

type InventoryResultEvent struct {
	BaseEvent
	OrderID          string `json:"order_id"`
	InventoryStatus  string `json:"inventory_status"`
	ReservedQuantity int    `json:"reserved_quantity"`
	Reason           string `json:"reason,omitempty"`
	SourceService    string `json:"source_service"`
}

type OrderStatusChangedEvent struct {
	BaseEvent
	OrderID       string `json:"order_id"`
	PreviousState string `json:"previous_state"`
	CurrentState  string `json:"current_state"`
	SourceService string `json:"source_service"`
}
