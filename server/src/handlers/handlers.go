package handlers

import (
	log "github.com/Sirupsen/logrus"

	collector_iface "github.com/vostrok/reporter/server/src/collector"
	"github.com/vostrok/reporter/server/src/config"
	"github.com/vostrok/utils/rec"
)

var collector collector_iface.Collector

func init() {
	log.SetLevel(log.DebugLevel)
}

func Init(appConfig config.AppConfig) {
	initMetrics()
	collector_iface.Init(appConfig.Collector)
}

type Response struct{}

type Rec struct {
}

func (rpc *Rec) IncMO(req rec.Record, res *Response) error {
	return collector.IncMO(req)
}
