package handlers

import (
	log "github.com/Sirupsen/logrus"

	collector_iface "github.com/vostrok/reporter/server/src/collector"
	"github.com/vostrok/reporter/server/src/config"
	m "github.com/vostrok/reporter/server/src/metrics"
	"github.com/vostrok/utils/rec"
)

var collector collector_iface.Collector

func init() {
	log.SetLevel(log.DebugLevel)
}

func Init(appConfig config.AppConfig) {
	m.Init(appConfig.AppName)
	collector_iface.Init(appConfig)
}

type Rec struct{}

type Response struct{}

func (rpc *Rec) IncMO(req rec.Record, res *Response) error {
	return collector.IncMO(req)
}
func (rpc *Rec) IncPixel(req rec.Record, res *Response) error {
	return collector.IncPixel(req)
}
func (rpc *Rec) IncHit(req rec.Record, res *Response) error {
	return collector.IncHit(req)
}
func (rpc *Rec) IncPaid(req rec.Record, res *Response) error {
	return collector.IncPaid(req)
}
