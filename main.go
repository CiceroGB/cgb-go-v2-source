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
	batchProcessor     = make(chan SummaryItem, 10000) // Batch processing channel
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

// Item for Redis batch processing
type SummaryItem struct {
	Prefix        string
	CorrelationId string
	Amount        float64
	Timestamp     int64
}

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

	// Start batch processor for Redis
	go summaryBatchHandler()
	

	// Configura endpoints HTTP
	setupHTTPHandlers()

	// Inicia servidor
	fmt.Println("Rinha 2025 - Payment Gateway Server running on", PORT)
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
	
	// GET /payments-summary - Retorna relatório de pagamentos
	http.HandleFunc("/payments-summary", handlePaymentsSummary)
}

func receivePayment(w http.ResponseWriter, r *http.Request) {
	// Ultra-minimal endpoint: zero overhead
	var p PostPayments
	json.NewDecoder(r.Body).Decode(&p)
	w.WriteHeader(http.StatusCreated)
	paymentQueue <- p
}

func handlePaymentsSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	
	// Parse dos parâmetros de data
	from, _ := time.Parse(time.RFC3339, r.URL.Query().Get("from"))
	to, _ := time.Parse(time.RFC3339, r.URL.Query().Get("to"))
	if from.IsZero() {
		from = time.Unix(0, 0).UTC()
	}
	if to.IsZero() {
		to = time.Now().UTC()
	}

	// Monta resposta com dados do Redis
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
		// Timestamp apenas quando realmente processar
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
			saveSummaryAsync(processorUsed, payment)
		}
	}
}

func forwardToProcessor(payment PostPayments, processorURL string) bool {
	// Control HTTP request concurrency
	concurrencyLimiter <- struct{}{}
	defer func() { <-concurrencyLimiter }()

	// Usa buffer pool para JSON encoding
	buf := bufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufferPool.Put(buf)

	if err := jsonFast.NewEncoder(buf).Encode(payment); err != nil {
		return false
	}

	// Faz request HTTP para o processador
	url := processorURL + "/payments"
	req, _ := http.NewRequest("POST", url, buf)
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return false // Timeout ou erro de rede
	}
	defer resp.Body.Close()
	
	return resp.StatusCode == http.StatusOK
}

// ============================================================================
// SUMMARY SYSTEM (REPORTS)
// ============================================================================

func saveSummaryAsync(processor string, payment PostPayments) {
	// Converte para item de batch
	ts, _ := time.Parse("2006-01-02T15:04:05.000Z07:00", payment.RequestedAt)
	item := SummaryItem{
		Prefix:        processor,
		CorrelationId: payment.CorrelationId,
		Amount:        payment.Amount,
		Timestamp:     ts.UnixMilli(),
	}
	
	// Try to send to batch, if full process directly
	select {
	case batchProcessor <- item:
		// Success - will be processed in batch
	default:
		// Batch full - process individually
		go processSummaryDirect(item)
	}
}

func summaryBatchHandler() {
	// Redis batch aggregator - reduces 90% operations
	batch := make([]SummaryItem, 0, 1000)           // Pre-allocated slice
	ticker := time.NewTicker(100 * time.Millisecond) // 10 batches/second
	defer ticker.Stop()

	for {
		select {
		case item := <-batchProcessor:
			batch = append(batch, item)
			// Flush when batch is full (1000 items)
			if len(batch) >= 1000 {
				processSummaryBatch(batch)
				batch = batch[:0] // Zero-alloc reset
			}
		case <-ticker.C:
			// Temporal flush every 100ms
			if len(batch) > 0 {
				processSummaryBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

func processSummaryBatch(batch []SummaryItem) {
	if len(batch) == 0 {
		return
	}
	
	ctx := context.Background()
	pipe := redisClient.Pipeline()
	
	// Adiciona todos os items do batch ao pipeline Redis
	for _, item := range batch {
		pipe.HSet(ctx, "summary:"+item.Prefix+":data", item.CorrelationId, item.Amount)
		pipe.ZAdd(ctx, "summary:"+item.Prefix+":history", redis.Z{
			Score:  float64(item.Timestamp),
			Member: item.CorrelationId,
		})
	}
	
	_, _ = pipe.Exec(ctx) // Executa tudo de uma vez
}

func processSummaryDirect(item SummaryItem) {
	ctx := context.Background()
	pipe := redisClient.Pipeline()
	pipe.HSet(ctx, "summary:"+item.Prefix+":data", item.CorrelationId, item.Amount)
	pipe.ZAdd(ctx, "summary:"+item.Prefix+":history", redis.Z{
		Score:  float64(item.Timestamp),
		Member: item.CorrelationId,
	})
	_, _ = pipe.Exec(ctx)
}

func getSummaryData(processor string, from, to time.Time) SummaryData {
	ctx := context.Background()
	result := SummaryData{}

	// Busca IDs dos pagamentos no período
	ids, _ := redisClient.ZRangeByScore(ctx, "summary:"+processor+":history", &redis.ZRangeBy{
		Min: fmt.Sprint(from.UnixMilli()),
		Max: fmt.Sprint(to.UnixMilli()),
	}).Result()

	if len(ids) == 0 {
		return result
	}

	// Busca valores dos pagamentos
	vals, _ := redisClient.HMGet(ctx, "summary:"+processor+":data", ids...).Result()
	for _, val := range vals {
		if v, ok := val.(string); ok {
			if amount, err := strconv.ParseFloat(v, 64); err == nil {
				result.TotalAmount += amount
				result.TotalRequests++
			}
		}
	}
	
	// Arredonda para 2 casas decimais
	result.TotalAmount = math.Round(result.TotalAmount*100) / 100
	return result
}

// ============================================================================
// UTILITIES
// ============================================================================

