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
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	jsoniter "github.com/json-iterator/go"
)

// ============================================================================
// CONFIGURATION AND TYPES
// ============================================================================

var (
	// Payment processor URLs
	PAYMENT_PROCESSOR_DEFAULT_URL  = getEnv("PAYMENT_PROCESSOR_DEFAULT_URL", "http://localhost:8001")
	PAYMENT_PROCESSOR_FALLBACK_URL = getEnv("PAYMENT_PROCESSOR_FALLBACK_URL", "http://localhost:8002")
	
	// Server configuration
	PORT      = getEnv("PORT", ":9999")
	REDIS_URL = getEnv("REDIS_URL", "127.0.0.1:6379")
	WORKERS   = getEnv("WORKERS", "30")

	// Core infrastructure
	paymentQueue = make(chan PostPayments, 100_000) // Payment processing queue
	dbClient     = redis.NewClient(&redis.Options{Addr: REDIS_URL})
	redisClient  = redis.NewClient(&redis.Options{Addr: REDIS_URL})
	
	// HTTP client with natural timeout
	httpClient = &http.Client{Timeout: 5 * time.Second}
	
	// Concurrency and performance control
	concurrencyLimiter = make(chan struct{}, 30)      // Concurrent request limiter
	bufferPool         = sync.Pool{New: func() interface{} { return &bytes.Buffer{} }}
	
	// Ultra-fast JSON for summary
	jsonFast = jsoniter.ConfigCompatibleWithStandardLibrary
	
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

// Direct Redis processing, no batching needed

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

// ============================================================================
// MAIN - SERVER INITIALIZATION
// ============================================================================

func main() {
	// Clean Redis on startup
	ctx := context.Background()
	_ = dbClient.FlushAll(ctx).Err()

	// Start payment processing workers
	workers, _ := strconv.Atoi(WORKERS)
	for i := 0; i < workers; i++ {
		go processPayments(paymentQueue)
	}

	// Direct Redis processing, no batch handler needed
	

	// Setup HTTP handlers
	setupHTTPHandlers()

	// Ensure PORT has colon prefix
	if !strings.HasPrefix(PORT, ":") {
		PORT = ":" + PORT
	}
	
	// Start server
	fmt.Println("Payment Gateway Server running on", PORT)
	if err := http.ListenAndServe(PORT, nil); err != nil {
		panic(err)
	}
}

// ============================================================================
// HTTP ENDPOINTS
// ============================================================================

func setupHTTPHandlers() {
	// POST /payments - Receive and process payments
	http.HandleFunc("/payments", receivePayment)
	
	// GET /payments-summary - Returns payment summary
	http.HandleFunc("/payments-summary", handlePaymentsSummary)
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
	select {
	case paymentQueue <- p:
		w.WriteHeader(http.StatusCreated)
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
	_ = jsonFast.NewEncoder(w).Encode(resp)
}

// ============================================================================
// PAYMENT PROCESSING
// ============================================================================

func processPayments(queue <-chan PostPayments) {
	for payment := range queue {
		payment.RequestedAt = time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00")

		// Try default processor with retry
		processed := false
		for i := 0; i < 5; i++ {
			if forwardToProcessor(payment, PAYMENT_PROCESSOR_DEFAULT_URL) {
				processed = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		
		// Save only once after processing succeeds
		if processed {
			saveSummaryAsync("default", payment)
		} else if forwardToProcessor(payment, PAYMENT_PROCESSOR_FALLBACK_URL) {
			saveSummaryAsync("fallback", payment)
		}
		// If both fail, don't save = perfect consistency
	}
}

func forwardToProcessor(payment PostPayments, processorURL string) bool {
	// Control HTTP request concurrency
	concurrencyLimiter <- struct{}{}
	defer func() { <-concurrencyLimiter }()

	// Use buffer pool for JSON encoding
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	if err := jsonFast.NewEncoder(buf).Encode(payment); err != nil {
		return false
	}

	// Make HTTP request to processor
	url := processorURL + "/payments"
	req, _ := http.NewRequest("POST", url, buf)
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	
	return resp.StatusCode == http.StatusOK
}

// ============================================================================
// SUMMARY SYSTEM (REPORTS)
// ============================================================================

func saveSummaryAsync(processor string, payment PostPayments) {
	ctx := context.Background()
	ts, _ := time.Parse("2006-01-02T15:04:05.000Z07:00", payment.RequestedAt)
	
	pipe := redisClient.Pipeline()
	pipe.HSet(ctx, "summary:"+processor+":data", payment.CorrelationId, payment.Amount)
	pipe.ZAdd(ctx, "summary:"+processor+":history", redis.Z{
		Score:  float64(ts.UnixMilli()),
		Member: payment.CorrelationId,
	})
	_, _ = pipe.Exec(ctx)
}

// Direct Redis processing for consistency

func getSummaryData(processor string, from, to time.Time) SummaryData {
	ctx := context.Background()
	result := SummaryData{}

	// Get payment IDs in time range
	ids, _ := redisClient.ZRangeByScore(ctx, "summary:"+processor+":history", &redis.ZRangeBy{
		Min: fmt.Sprint(from.UnixMilli()),
		Max: fmt.Sprint(to.UnixMilli()),
	}).Result()

	if len(ids) == 0 {
		return result
	}

	// Get payment amounts
	vals, _ := redisClient.HMGet(ctx, "summary:"+processor+":data", ids...).Result()
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

// ============================================================================
// UTILITIES
// ============================================================================

