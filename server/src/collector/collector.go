package collector

import (
	"database/sql"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	acceptor_client "github.com/vostrok/acceptor/rpcclient"
	acceptor "github.com/vostrok/acceptor/server/src/base"
	"github.com/vostrok/reporter/server/src/config"
	rec "github.com/vostrok/utils/rec"
)

//Provider > Operator > Campaign > ReportDate

//ReportDate   int64  `json:"report_date,omitempty"`
//Campaign     int32  `json:"id_campaign,omitempty"`
//Provider     string `json:"id_provider,omitempty"`
//Operator     int32  `json:"id_operator,omitempty"`
//LPHits       int32  `json:"total_lp_hits,omitempty"`
//LPMsisdnHits int32  `json:"total_lp_msisdn_hits,omitempty"`
//Mo           int32  `json:"total_mo,omitempty"`
//MoUniq       int32  `json:"total_mo_uniq,omitempty"`
//MoSuccess    int32  `json:"total_mo_success_charge,omitempty"`
//Pixels       int32  `json:"total_pixels_sent,omitempty"`

var svc *collectorService

type Collector interface {
	IncMO(r rec.Record) error // mo, mo uniq, mo success
	IncPixel(r rec.Record) error
	IncHit(r rec.Record) error // both lp hit and lp msisdn hit
}

type collectorService struct {
	conf     config.CollectorConfig
	db       *sql.DB
	adReport map[int64]OperatorAgregate // map[campaign][operator]acceptor.Aggregate
}

type OperatorAgregate map[int64]acceptor.Aggregate
type CampaignAggregate map[int64]OperatorAgregate

type LpHits struct {
	sync.RWMutex
	count int64
}
type LpMsisdnHits struct {
	sync.RWMutex
	count int64
}
type MO struct {
	sync.RWMutex
	count int64
}
type MOSuccess struct {
	sync.RWMutex
	count int64
}
type Pixels struct {
	sync.RWMutex
	count int64
}

func Init(acceptorClient acceptor_client.ClientConfig) *Collector {
	as := &collectorService{}
	if err := acceptor_client.Init(acceptorClient); err != nil {
		log.Fatal("cannot init acceptor client")
	}
	as.adReport = make(map[int64]OperatorAgregate)
	go func() {
		for range time.Tick(time.Second) {
			as.send()
		}
	}()

	return as
}

func (as *collectorService) send() {
	var data []acceptor.Aggregate
	if err := acceptor_client.SendAggregatedData(data); err != nil {
		log.WithFields(log.Fields{"error": err.Error()}).Error("cannot send data")
	} else {
		// clean
	}
}
