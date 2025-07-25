package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
)

// Configuration
var (
	PORT = getEnv("PORT", ":9999")
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

func handlePayments(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	
	var p PostPayments
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	
	// TODO: Process payment
	w.WriteHeader(http.StatusAccepted)
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

func main() {
	http.HandleFunc("/payments", handlePayments)
	http.HandleFunc("/payments-summary", handlePaymentsSummary)
	
	fmt.Println("Payment Gateway Server running on", PORT)
	if err := http.ListenAndServe(PORT, nil); err != nil {
		panic(err)
	}
}