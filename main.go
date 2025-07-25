package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
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
	paymentQueue       = make(chan PostPayments, 100_000)
	dbClient           = redis.NewClient(&redis.Options{Addr: REDIS_URL})
	httpClient         = &http.Client{Timeout: 5 * time.Second}
	concurrencyLimiter = make(chan struct{}, 30)
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
	
	// Parse date parameters
	from, _ := time.Parse(time.RFC3339, r.URL.Query().Get("from"))
	to, _ := time.Parse(time.RFC3339, r.URL.Query().Get("to"))
	if from.IsZero() {
		from = time.Unix(0, 0).UTC()
	}
	if to.IsZero() {
		to = time.Now().UTC()
	}

	// Build response with Redis data
	resp := PaymentsSummary{
		Default:  getSummaryData("default", from, to),
		Fallback: getSummaryData("fallback", from, to),
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func getSummaryData(processor string, from, to time.Time) SummaryData {
	ctx := context.Background()
	result := SummaryData{}

	// Get payment IDs in the time range
	ids, _ := dbClient.ZRangeByScore(ctx, "summary:"+processor+":history", &redis.ZRangeBy{
		Min: fmt.Sprint(from.UnixMilli()),
		Max: fmt.Sprint(to.UnixMilli()),
	}).Result()

	if len(ids) == 0 {
		return result
	}

	// Get payment amounts
	vals, _ := dbClient.HMGet(ctx, "summary:"+processor+":data", ids...).Result()
	for _, val := range vals {
		if v, ok := val.(string); ok {
			if amount, err := strconv.ParseFloat(v, 64); err == nil {
				result.TotalAmount += amount
				result.TotalRequests++
			}
		}
	}
	
	// Round to 2 decimal places
	result.TotalAmount = math.Round(result.TotalAmount*100) / 100
	return result
}

func processPayments(queue <-chan PostPayments) {
	for payment := range queue {
		// Update timestamp for processing
		payment.RequestedAt = time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00")

		// Strategy: Default first (lower fee = more profit)
		processorUsed := "none"
		if forwardToProcessor(payment, PAYMENT_PROCESSOR_DEFAULT_URL) {
			processorUsed = "default"
		} else if forwardToProcessor(payment, PAYMENT_PROCESSOR_FALLBACK_URL) {
			processorUsed = "fallback"
		}
		
		// Save result if processed successfully
		if processorUsed != "none" {
			saveSummaryData(processorUsed, payment)
		}
	}
}

func forwardToProcessor(payment PostPayments, processorURL string) bool {
	// Control HTTP request concurrency
	concurrencyLimiter <- struct{}{}
	defer func() { <-concurrencyLimiter }()

	// Create JSON payload
	body, err := json.Marshal(payment)
	if err != nil {
		return false
	}

	// Make HTTP request to processor
	url := processorURL + "/payments"
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	
	return resp.StatusCode == http.StatusOK
}

func saveSummaryData(processor string, payment PostPayments) {
	ctx := context.Background()
	t, _ := time.Parse("2006-01-02T15:04:05.000Z07:00", payment.RequestedAt)
	
	pipe := dbClient.Pipeline()
	pipe.HSet(ctx, "summary:"+processor+":data", payment.CorrelationId, payment.Amount)
	pipe.ZAdd(ctx, "summary:"+processor+":history", redis.Z{
		Score:  float64(t.UnixMilli()),
		Member: payment.CorrelationId,
	})
	_, _ = pipe.Exec(ctx)
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