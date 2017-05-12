package collector

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	acceptor_client "github.com/linkit360/go-acceptor-client"
	acceptor "github.com/linkit360/go-acceptor-structs"
	"github.com/linkit360/go-reporter/server/src/config"
	m "github.com/linkit360/go-reporter/server/src/metrics"
	"strings"
)

type Collector interface {
	IncPixel(r Collect) error
	IncHit(r Collect) error
	IncTransaction(r Collect) error
}

type Collect struct {
	CampaignId        int64  `json:"id_campaign,omitempty"`
	OperatorCode      int64  `json:"operator_code,omitempty"`
	Msisdn            string `json:"msisdn,omitempty"`
	TransactionResult string `json:"transaction_result,omitempty"`
	Price             int    `json:"price,omitempty"`
	AttemptsCount     int    `json:"attempts_count,omitempty"`
}

type collectorService struct {
	sync.RWMutex
	conf     config.CollectorConfig
	db       *sql.DB
	adReport map[int64]OperatorAgregate // map[campaign][operator]acceptor.Aggregate

}

type OperatorAgregate map[int64]adAggregate

type adAggregate struct {
	LpHits               *counter `json:"lp_hits,omitempty"`
	LpMsisdnHits         *counter `json:"lp_msisdn_hits,omitempty"`
	MoTotal              *counter `json:"mo,omitempty"`
	MoChargeSuccess      *counter `json:"mo_charge_success,omitempty"`
	MoChargeSum          *counter `json:"mo_charge_sum,omitempty"`
	MoChargeFailed       *counter `json:"mo_charge_failed,omitempty"`
	MoRejected           *counter `json:"mo_rejected,omitempty"`
	RenewalTotal         *counter `json:"renewal,omitempty"`
	RenewalChargeSuccess *counter `json:"renewal_charge_success,omitempty"`
	RenewalChargeSum     *counter `json:"renewal_charge_sum,omitempty"`
	RenewalFailed        *counter `json:"renewal_failed,omitempty"`
	Pixels               *counter `json:"pixels,omitempty"`
}

type counter struct {
	count int64
}

func (c *counter) Inc() {
	c.count++
}
func (c *counter) Add(amount int) {
	c.count = c.count + int64(amount)
}

func (a *adAggregate) Sum() int64 {
	return a.LpHits.count +
		a.LpMsisdnHits.count +
		a.MoTotal.count +
		a.MoChargeSuccess.count +
		a.MoChargeSum.count +
		a.MoChargeFailed.count +
		a.MoRejected.count +
		a.RenewalTotal.count +
		a.RenewalChargeSuccess.count +
		a.RenewalChargeSum.count +
		a.RenewalFailed.count +
		a.Pixels.count
}

func Init(appConfig config.AppConfig) Collector {
	as := &collectorService{
		conf: appConfig.Collector,
	}
	if err := acceptor_client.Init(appConfig.AcceptorClient); err != nil {
		m.Errors.Inc()
		log.Error("cannot init acceptor client")
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
	as.Lock()
	defer as.Unlock()
	begin := time.Now()
	var data []acceptor.Aggregate
	aggregateSum := int64(.0)
	for campaignId, operatorAgregate := range as.adReport {
		for operatorCode, coa := range operatorAgregate {
			if coa.Sum() == 0 {
				continue
			}
			aggregateSum = aggregateSum + coa.Sum()

			aa := acceptor.Aggregate{
				ReportAt:             time.Now().Unix(),
				ProviderName:         as.conf.Provider,
				CampaignId:           campaignId,
				OperatorCode:         operatorCode,
				LpHits:               coa.LpHits.count,
				LpMsisdnHits:         coa.LpMsisdnHits.count,
				MoTotal:              coa.MoTotal.count,
				MoChargeSuccess:      coa.MoChargeSuccess.count,
				MoChargeSum:          coa.MoChargeSum.count,
				MoChargeFailed:       coa.MoChargeFailed.count,
				MoRejected:           coa.MoRejected.count,
				RenewalTotal:         coa.RenewalTotal.count,
				RenewalChargeSuccess: coa.RenewalChargeSuccess.count,
				RenewalChargeSum:     coa.RenewalChargeSum.count,
				RenewalFailed:        coa.RenewalFailed.count,
				Pixels:               coa.Pixels.count,
			}
			data = append(data, aa)
		}
	}
	if len(data) > 0 {
		log.WithFields(log.Fields{"took": time.Since(begin)}).Info("prepare")
		if err := acceptor_client.SendAggregatedData(data); err != nil {
			m.Errors.Inc()
			log.WithFields(log.Fields{"error": err.Error()}).Error("cannot send data")
		} else {
			body, _ := json.Marshal(data)
			log.WithFields(log.Fields{"data": string(body)}).Debug("sent")
		}
		as.breathe()
	}
	m.SendDuration.Observe(time.Since(begin).Seconds())
	m.AggregateSum.Observe(float64(aggregateSum))
}

func (as *collectorService) breathe() {
	begin := time.Now()
	for campaignId, operatorAgregate := range as.adReport {
		for operatorCode, _ := range operatorAgregate {
			delete(as.adReport[campaignId], operatorCode)
		}
		delete(as.adReport, campaignId)
	}
	log.WithFields(log.Fields{"took": time.Since(begin)}).Debug("breathe")
	m.BreatheDuration.Observe(time.Since(begin).Seconds())
}

// map[campaign][operator]acceptor.Aggregate
func (as *collectorService) check(r Collect) error {
	if r.CampaignId == 0 {
		m.Errors.Inc()
		m.ErrorCampaignIdEmpty.Inc()
		return fmt.Errorf("CampaignIdEmpty: %#v", r)

	}
	if r.OperatorCode == 0 {
		m.Errors.Inc()
		m.ErrorCampaignIdEmpty.Inc()
	}
	as.Lock()
	defer as.Unlock()
	// operator code == 0
	// unknown operator in access campaign
	if as.adReport == nil {
		as.adReport = make(map[int64]OperatorAgregate)
	}
	_, found := as.adReport[r.CampaignId]
	if !found {
		as.adReport[r.CampaignId] = OperatorAgregate{}
	}
	_, found = as.adReport[r.CampaignId][r.OperatorCode]
	if !found {
		as.adReport[r.CampaignId][r.OperatorCode] = adAggregate{
			LpHits:               &counter{},
			LpMsisdnHits:         &counter{},
			MoTotal:              &counter{},
			MoChargeSuccess:      &counter{},
			MoChargeSum:          &counter{},
			MoChargeFailed:       &counter{},
			MoRejected:           &counter{},
			RenewalTotal:         &counter{},
			RenewalChargeSuccess: &counter{},
			RenewalChargeSum:     &counter{},
			RenewalFailed:        &counter{},
			Pixels:               &counter{},
		}
	}
	return nil
}

// both lp hit and lp msisdn hit
func (as *collectorService) IncHit(r Collect) error {
	if err := as.check(r); err != nil {
		return err
	}
	as.Lock()
	defer as.Unlock()
	as.adReport[r.CampaignId][r.OperatorCode].LpHits.Inc()
	if r.Msisdn != "" {
		as.adReport[r.CampaignId][r.OperatorCode].LpMsisdnHits.Inc()
	}
	return nil
}

func (as *collectorService) IncTransaction(r Collect) error {
	if err := as.check(r); err != nil {
		return err
	}
	as.Lock()
	defer as.Unlock()

	if r.AttemptsCount == 0 {
		as.adReport[r.CampaignId][r.OperatorCode].MoTotal.Inc()
		if r.TransactionResult == "paid" {
			as.adReport[r.CampaignId][r.OperatorCode].MoChargeSuccess.Inc()
			as.adReport[r.CampaignId][r.OperatorCode].MoChargeSum.Add(r.Price)
		}
		if r.TransactionResult == "rejected" {
			as.adReport[r.CampaignId][r.OperatorCode].MoRejected.Inc()
		}
		if r.TransactionResult == "failed" {
			as.adReport[r.CampaignId][r.OperatorCode].MoChargeFailed.Inc()
		}
		return nil
	}

	as.adReport[r.CampaignId][r.OperatorCode].RenewalTotal.Inc()

	if strings.Contains(r.TransactionResult, "paid") {
		as.adReport[r.CampaignId][r.OperatorCode].RenewalChargeSuccess.Inc()
		as.adReport[r.CampaignId][r.OperatorCode].RenewalChargeSum.Add(r.Price)
	}

	if strings.Contains(r.TransactionResult, "failed") {
		as.adReport[r.CampaignId][r.OperatorCode].RenewalFailed.Inc()
	}
	return nil
}

func (as *collectorService) IncPixel(r Collect) error {
	if err := as.check(r); err != nil {
		return err
	}
	as.Lock()
	defer as.Unlock()
	as.adReport[r.CampaignId][r.OperatorCode].Pixels.Inc()
	return nil
}
