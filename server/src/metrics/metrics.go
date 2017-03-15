package handlers

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	m "github.com/vostrok/utils/metrics"
)

var (
	Success m.Gauge
	Errors  m.Gauge

	ErrorCampaignIdEmpty m.Gauge

	BreatheDuration prometheus.Summary
	SendDuration    prometheus.Summary
)

func Init(appName string) {
	Success = m.NewGauge("", "", "success", "success")
	Errors = m.NewGauge("", "", "errors", "errors")

	ErrorCampaignIdEmpty = m.NewGauge("errors", "campaign_id", "empty", "errors")

	BreatheDuration = m.NewSummary(appName+"_breathe_duration_seconds", "breathe duration seconds")
	SendDuration = m.NewSummary(appName+"_send_duration_seconds", "send duration seconds")

	go func() {
		for range time.Tick(time.Minute) {
			Success.Update()
			Errors.Update()
		}
	}()
}
