package handlers

import (
	log "github.com/Sirupsen/logrus"

	collector_iface "github.com/linkit360/go-reporter/server/src/collector"
	"github.com/linkit360/go-reporter/server/src/config"
	m "github.com/linkit360/go-reporter/server/src/metrics"
)

var collector collector_iface.Collector

func init() {
	log.SetLevel(log.DebugLevel)
}

func Init(appConfig config.AppConfig) {
	m.Init(appConfig.AppName)
	collector = collector_iface.Init(appConfig)
}

type Collect struct{}

type Response struct{}

func (rpc *Collect) IncMO(req collector_iface.Collect, res *Response) error {
	return collector.IncMO(req)
}
func (rpc *Collect) IncPixel(req collector_iface.Collect, res *Response) error {
	return collector.IncPixel(req)
}
func (rpc *Collect) IncHit(req collector_iface.Collect, res *Response) error {
	return collector.IncHit(req)
}
func (rpc *Collect) IncPaid(req collector_iface.Collect, res *Response) error {
	return collector.IncPaid(req)
}
