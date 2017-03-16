package collector

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	acceptor_client "github.com/vostrok/acceptor/rpcclient"
	acceptor "github.com/vostrok/acceptor/server/src/base"
	"github.com/vostrok/reporter/server/src/config"
	m "github.com/vostrok/reporter/server/src/metrics"
)

type Collector interface {
	IncMO(r Collect) error // mo, mo uniq, mo success
	IncPixel(r Collect) error
	IncHit(r Collect) error // both lp hit and lp msisdn hit
	IncPaid(r Collect) error
}

type collectorService struct {
	conf     config.CollectorConfig
	db       *sql.DB
	adReport map[int64]OperatorAgregate // map[campaign][operator]acceptor.Aggregate
}

type Collect struct {
	CampaignId        int64  `json:"id_campaign"`
	OperatorCode      int64  `json:"operator_code"`
	Msisdn            string `json:"msisdn"`
	TransactionResult string `json:"transaction_result"`
}

type OperatorAgregate map[int64]adAggregate

type adAggregate struct {
	LpHits       *lpHits
	LpMsisdnHits *lpMsisdnHits
	MOTotal      *moTotal
	MOSuccess    *moSuccess
	RetrySuccess *retrySuccess
	MOUniq       *moUniq
	Pixels       *pixels
}

type lpHits struct {
	sync.RWMutex
	count int64
}

func (lh *lpHits) Inc() {
	lh.Lock()
	defer lh.Unlock()
	lh.count++
}

type lpMsisdnHits struct {
	sync.RWMutex
	count int64
}

func (lmh *lpMsisdnHits) Inc() {
	lmh.Lock()
	defer lmh.Unlock()
	lmh.count++
}

type moTotal struct {
	sync.RWMutex
	count int64
}

func (mo *moTotal) Inc() {
	mo.Lock()
	defer mo.Unlock()
	mo.count++
}

type moSuccess struct {
	sync.RWMutex
	count int64
}

func (mo *moSuccess) Inc() {
	mo.Lock()
	defer mo.Unlock()
	mo.count++
}

type retrySuccess struct {
	sync.RWMutex
	count int64
}

func (retry *retrySuccess) Inc() {
	retry.Lock()
	defer retry.Unlock()
	retry.count++
}

type moUniq struct {
	sync.RWMutex
	uniq map[string]struct{}
}

func (mo *moUniq) Track(msisdn string) {
	mo.Lock()
	defer mo.Unlock()
	if mo.uniq == nil {
		mo.uniq = make(map[string]struct{})
	}
	mo.uniq[msisdn] = struct{}{}
}

type pixels struct {
	sync.RWMutex
	count int64
}

func (p *pixels) Inc() {
	p.Lock()
	defer p.Unlock()
	p.count++
}

func Init(appConfig config.AppConfig) Collector {
	as := &collectorService{}
	if err := acceptor_client.Init(appConfig.AcceptorClient); err != nil {
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
	begin := time.Now()
	var data []acceptor.Aggregate
	for campaignId, operatorAgregate := range as.adReport {
		for operatorCode, intAggregate := range operatorAgregate {
			aa := acceptor.Aggregate{
				ReportDate:   time.Now().Unix(),
				Provider:     as.conf.Provider,
				CampaignId:   campaignId,
				OperatorCode: operatorCode,
				LPHits:       intAggregate.LpHits.count,
				LPMsisdnHits: intAggregate.LpMsisdnHits.count,
				Mo:           intAggregate.MOTotal.count,
				MoSuccess:    intAggregate.MOSuccess.count,
				RetrySuccess: intAggregate.RetrySuccess.count,
				MoUniq:       int64(len(intAggregate.MOUniq.uniq)),
				Pixels:       intAggregate.Pixels.count,
			}
			data = append(data, aa)
		}
	}
	log.WithFields(log.Fields{"took": time.Since(begin)}).Info("prepare")
	if err := acceptor_client.SendAggregatedData(data); err != nil {
		log.WithFields(log.Fields{"error": err.Error()}).Error("cannot send data")
	} else {
		as.breathe()
	}
	m.SendDuration.Observe(time.Since(begin).Seconds())
}

func (as *collectorService) breathe() {
	begin := time.Now()
	for campaignId, operatorAgregate := range as.adReport {
		for operatorCode, _ := range operatorAgregate {
			delete(as.adReport[campaignId], operatorCode)
		}
		delete(as.adReport, campaignId)
	}
	log.WithFields(log.Fields{"took": time.Since(begin)}).Info("breathe")
	m.BreatheDuration.Observe(time.Since(begin).Seconds())
}

// map[campaign][operator]acceptor.Aggregate
func (as *collectorService) check(r Collect) error {
	if r.CampaignId == 0 {
		m.ErrorCampaignIdEmpty.Inc()
		return fmt.Errorf("CampaignIdEmpty: %#v", r)

	}
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
			LpHits:       &lpHits{},
			LpMsisdnHits: &lpMsisdnHits{},
			MOTotal:      &moTotal{},
			MOSuccess:    &moSuccess{},
			RetrySuccess: &retrySuccess{},
			MOUniq:       &moUniq{},
			Pixels:       &pixels{},
		}
	}
	return nil
}

// both lp hit and lp msisdn hit
func (as *collectorService) IncHit(r Collect) error {
	if err := as.check(r); err != nil {
		return err
	}
	as.adReport[r.CampaignId][r.OperatorCode].LpHits.Inc()
	if r.Msisdn != "" {
		as.adReport[r.CampaignId][r.OperatorCode].LpMsisdnHits.Inc()
	}
	return nil
}

// mo, mo uniq, mo success
func (as *collectorService) IncMO(r Collect) error {
	if err := as.check(r); err != nil {
		return err
	}
	as.adReport[r.CampaignId][r.OperatorCode].MOTotal.Inc()
	as.adReport[r.CampaignId][r.OperatorCode].MOUniq.Track(r.Msisdn)
	return nil
}
func (as *collectorService) IncPaid(r Collect) error {
	if err := as.check(r); err != nil {
		return err
	}
	if r.TransactionResult == "paid" {
		as.adReport[r.CampaignId][r.OperatorCode].MOSuccess.Inc()
	}
	if r.TransactionResult == "retry_paid" {
		as.adReport[r.CampaignId][r.OperatorCode].RetrySuccess.Inc()
	}
	return nil
}

func (as *collectorService) IncPixel(r Collect) error {
	if err := as.check(r); err != nil {
		return err
	}
	as.adReport[r.CampaignId][r.OperatorCode].Pixels.Inc()
	return nil
}
