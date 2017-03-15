package handlers

import (
	"time"

	m "github.com/vostrok/utils/metrics"
)

var (
	success m.Gauge
	errors  m.Gauge
)

func initMetrics() {
	success = m.NewGauge("", "", "success", "success")
	errors = m.NewGauge("", "", "errors", "errors")

	go func() {
		for range time.Tick(time.Minute) {
			success.Update()
			errors.Update()
		}
	}()
}
