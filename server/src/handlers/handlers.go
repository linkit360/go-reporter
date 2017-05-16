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

func OnExit() {
	collector.SaveState()
}

type Collect struct{}

type Response struct{}

func (rpc *Collect) IncPixel(req collector_iface.Collect, res *Response) error {
	collector.IncPixel(req)
	return nil
}

func (rpc *Collect) IncHit(req collector_iface.Collect, res *Response) error {
	collector.IncHit(req)
	return nil
}

func (rpc *Collect) IncTransaction(req collector_iface.Collect, res *Response) error {
	collector.IncTransaction(req)
	return nil
}

func (rpc *Collect) IncOutflow(req collector_iface.Collect, res *Response) error {
	collector.IncOutflow(req)
	return nil
}
