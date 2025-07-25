package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Configuration
var (
	PAYMENT_PROCESSOR_DEFAULT_URL  = getEnv("PAYMENT_PROCESSOR_DEFAULT_URL", "http://localhost:8001")
	PAYMENT_PROCESSOR_FALLBACK_URL = getEnv("PAYMENT_PROCESSOR_FALLBACK_URL", "http://localhost:8002")
	PORT                           = getEnv("PORT", ":9999")
	REDIS_URL                      = getEnv("REDIS_URL", "127.0.0.1:6379")
	WORKERS                        = getEnv("WORKERS", "30")

	// Core infrastructure
	paymentQueue = make(chan PostPayments, 100_000)
	dbClient     = redis.NewClient(&redis.Options{Addr: REDIS_URL})
	httpClient   = &http.Client{Timeout: 30 * time.Second}
)

// Payment structure
type PostPayments struct {
	CorrelationId string  `json:"correlationId"`
	Amount        float64 `json:"amount"`
	RequestedAt   string  `json:"requestedAt"`
}

// Summary data structure
type SummaryData struct {
	TotalRequests int64   `json:"totalRequests"`
	TotalAmount   float64 `json:"totalAmount"`
}

// Response structure for /payments-summary endpoint
type PaymentsSummary struct {
	Default  SummaryData `json:"default"`
	Fallback SummaryData `json:"fallback"`
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func receivePayment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	
	var p PostPayments
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	
	// Add to processing queue
	p.RequestedAt = time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00")
	select {
	case paymentQueue <- p:
		w.WriteHeader(http.StatusAccepted)
	default:
		w.WriteHeader(http.StatusTooManyRequests)
	}
}

func handlePaymentsSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	
	// TODO: Implement summary logic
	resp := PaymentsSummary{
		Default:  SummaryData{},
		Fallback: SummaryData{},
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func processPayments(queue <-chan PostPayments) {
	for payment := range queue {
		// TODO: Implement payment processing logic
		fmt.Printf("Processing payment: %s\n", payment.CorrelationId)
	}
}

func main() {
	// Clean Redis on startup
	ctx := context.Background()
	_ = dbClient.FlushAll(ctx).Err()

	// Start payment processing workers
	workers, _ := strconv.Atoi(WORKERS)
	for i := 0; i < workers; i++ {
		go processPayments(paymentQueue)
	}

	http.HandleFunc("/payments", receivePayment)
	http.HandleFunc("/payments-summary", handlePaymentsSummary)
	
	fmt.Println("Payment Gateway Server running on", PORT)
	if err := http.ListenAndServe(PORT, nil); err != nil {
		panic(err)
	}
}