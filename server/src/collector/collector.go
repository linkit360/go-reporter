package collector

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	acceptor_client "github.com/linkit360/go-acceptor-client"
	acceptor "github.com/linkit360/go-acceptor-structs"
	"github.com/linkit360/go-reporter/server/src/config"
	m "github.com/linkit360/go-reporter/server/src/metrics"
)

type Collector interface {
	SaveState()
	IncPixel(r Collect) error
	IncHit(r Collect) error
	IncTransaction(r Collect) error
	IncOutflow(r Collect) error
}

type Collect struct {
	Tid               string `json:"tid,omitempty"`
	CampaignCode      string `json:"campaign_code,omitempty"`
	OperatorCode      int64  `json:"operator_code,omitempty"`
	Msisdn            string `json:"msisdn,omitempty"`
	TransactionResult string `json:"transaction_result,omitempty"`
	Price             int    `json:"price,omitempty"`
	AttemptsCount     int    `json:"attempts_count,omitempty"`
}

type collectorService struct {
	sync.RWMutex
	conf     config.CollectorConfig
	state    CollectorState
	db       *sql.DB
	adReport map[string]OperatorAgregate // map[campaign][operator]acceptor.Aggregate
}

type OperatorAgregate map[int64]adAggregate

type CollectorState struct {
	LastSendTime time.Time            `json:"last_send_time"`
	Archive      []acceptor.Aggregate `json:"archive"`
}

type adAggregate struct {
	LpHits                 *counter `json:"lp_hits,omitempty"`
	LpMsisdnHits           *counter `json:"lp_msisdn_hits,omitempty"`
	MoTotal                *counter `json:"mo,omitempty"`
	MoChargeSuccess        *counter `json:"mo_charge_success,omitempty"`
	MoChargeSum            *counter `json:"mo_charge_sum,omitempty"`
	MoChargeFailed         *counter `json:"mo_charge_failed,omitempty"`
	MoRejected             *counter `json:"mo_rejected,omitempty"`
	Outflow                *counter `json:"outflow,omitempty"`
	RenewalTotal           *counter `json:"renewal,omitempty"`
	RenewalChargeSuccess   *counter `json:"renewal_charge_success,omitempty"`
	RenewalChargeSum       *counter `json:"renewal_charge_sum,omitempty"`
	RenewalFailed          *counter `json:"renewal_failed,omitempty"`
	InjectionTotal         *counter `json:"injection,omitempty"`
	InjectionChargeSuccess *counter `json:"injection_charge_success,omitempty"`
	InjectionChargeSum     *counter `json:"injection_charge_sum,omitempty"`
	InjectionFailed        *counter `json:"injection_failed,omitempty"`
	ExpiredTotal           *counter `json:"expired,omitempty"`
	ExpiredChargeSuccess   *counter `json:"expired_charge_success,omitempty"`
	ExpiredChargeSum       *counter `json:"expired_charge_sum,omitempty"`
	ExpiredFailed          *counter `json:"expired_failed,omitempty"`
	Pixels                 *counter `json:"pixels,omitempty"`
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

		a.ExpiredTotal.count +
		a.ExpiredChargeSuccess.count +
		a.ExpiredChargeSum.count +
		a.ExpiredFailed.count +

		a.InjectionTotal.count +
		a.InjectionChargeSuccess.count +
		a.InjectionChargeSum.count +
		a.InjectionFailed.count +

		a.Outflow.count +
		a.Pixels.count
}

func Init(appConfig config.AppConfig) Collector {
	as := &collectorService{
		conf: appConfig.Collector,
	}
	if err := as.loadState(); err != nil {
		log.WithField("error", err.Error()).Fatal("cannot load state")
	}

	if err := acceptor_client.Init(appConfig.AcceptorClient); err != nil {
		m.Errors.Inc()
		log.Error("cannot init acceptor client")
	}

	as.adReport = make(map[string]OperatorAgregate)
	go func() {
		for range time.Tick(time.Second) {
			as.send()
		}
	}()

	return as
}

func (as *collectorService) SaveState() {
	if err := as.saveState(); err != nil {
		log.WithField("error", err.Error()).Fatal("cannot save state")
	}
}
func (as *collectorService) saveState() error {
	stateJson, err := json.Marshal(as.state)
	if err != nil {
		err = fmt.Errorf("json.Marshal: %s", err.Error())
		return err
	}

	if err := ioutil.WriteFile(as.conf.StateConfigFilePath, stateJson, 0644); err != nil {
		err = fmt.Errorf("ioutil.WriteFile: %s", err.Error())
		return err
	}
	return nil
}

func (as *collectorService) loadState() error {
	logCtx := log.WithField("action", "load collector state")
	stateJson, err := ioutil.ReadFile(as.conf.StateConfigFilePath)
	if err != nil {
		err = fmt.Errorf("ioutil.ReadFile: %s", err.Error())
		logCtx.WithField("path", as.conf.StateConfigFilePath).Error(err.Error())
		return err
	}
	if err = json.Unmarshal(stateJson, &as.state); err != nil {
		err = fmt.Errorf("json.Unmarshal: %s", err.Error())
		logCtx.Error(err.Error())
		return err
	}
	log.Debug("checking time")
	if as.state.LastSendTime.IsZero() {
		as.state.LastSendTime = time.Now().UTC()
		logCtx.Warn("invalid time")
	}
	logCtx.Infof("%s, count: %s", as.state.LastSendTime.String(), len(as.state.Archive))
	return nil
}

func (as *collectorService) send() {
	as.Lock()
	defer as.Unlock()

	begin := time.Now()
	var data []acceptor.Aggregate
	aggregateSum := int64(.0)

	for campaignCode, operatorAgregate := range as.adReport {
		for operatorCode, coa := range operatorAgregate {
			if coa.Sum() == 0 {
				continue
			}

			aggregateSum = aggregateSum + coa.Sum()

			aa := acceptor.Aggregate{
				ReportAt:               time.Now().Unix(),
				ProviderName:           as.conf.Provider,
				CampaignCode:           campaignCode,
				OperatorCode:           operatorCode,
				LpHits:                 coa.LpHits.count,
				LpMsisdnHits:           coa.LpMsisdnHits.count,
				MoTotal:                coa.MoTotal.count,
				MoChargeSuccess:        coa.MoChargeSuccess.count,
				MoChargeSum:            coa.MoChargeSum.count,
				MoChargeFailed:         coa.MoChargeFailed.count,
				MoRejected:             coa.MoRejected.count,
				Outflow:                coa.Outflow.count,
				RenewalTotal:           coa.RenewalTotal.count,
				RenewalChargeSuccess:   coa.RenewalChargeSuccess.count,
				RenewalChargeSum:       coa.RenewalChargeSum.count,
				RenewalFailed:          coa.RenewalFailed.count,
				InjectionTotal:         coa.InjectionTotal.count,
				InjectionChargeSuccess: coa.InjectionChargeSuccess.count,
				InjectionChargeSum:     coa.InjectionChargeSum.count,
				InjectionFailed:        coa.InjectionFailed.count,
				ExpiredTotal:           coa.ExpiredTotal.count,
				ExpiredChargeSuccess:   coa.ExpiredChargeSuccess.count,
				ExpiredChargeSum:       coa.ExpiredChargeSum.count,
				ExpiredFailed:          coa.ExpiredFailed.count,

				Pixels: coa.Pixels.count,
			}
			data = append(data, aa)
		}
	}
	as.state.Archive = append(as.state.Archive, data...)

	if len(as.state.Archive) > 0 {
		log.WithFields(log.Fields{"took": time.Since(begin)}).Info("prepare")
		resp, err := acceptor_client.SendAggregatedData(as.state.Archive)
		if err != nil || !resp.Ok {
			if err != nil {
				m.Errors.Inc()
				log.WithFields(log.Fields{"error": err.Error()}).Error("cannot send data")
			} else {
				if !resp.Ok {
					log.WithFields(log.Fields{"reason": resp.Error}).Warn("haven't received the data")
				}
			}

			log.WithFields(log.Fields{"count": len(data)}).Debug("added data to the archive")
		} else {
			queueJson, _ := json.Marshal(as.state.Archive)
			log.WithFields(log.Fields{
				"count": len(as.state.Archive),
				"data":  string(queueJson),
			}).Debug("sent")
			as.state.Archive = []acceptor.Aggregate{}
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
	if r.CampaignCode == "" {
		m.Errors.Inc()
		m.ErrorCampaignIdEmpty.Inc()

		log.WithField("collect", fmt.Sprintf("%#v", r)).Error("campaign code is empty")
		return fmt.Errorf("CampaignIdEmpty: %#v", r)

	}

	if r.OperatorCode == 0 {
		m.Errors.Inc()
		m.ErrorOperatorCodeEmpty.Inc()
		log.WithField("collect", fmt.Sprintf("%#v", r)).Error("operator code is empty")
	}
	as.Lock()
	defer as.Unlock()
	// operator code == 0
	// unknown operator in access campaign
	if as.adReport == nil {
		as.adReport = make(map[string]OperatorAgregate)
	}
	_, found := as.adReport[r.CampaignCode]
	if !found {
		as.adReport[r.CampaignCode] = OperatorAgregate{}
	}
	_, found = as.adReport[r.CampaignCode][r.OperatorCode]
	if !found {
		as.adReport[r.CampaignCode][r.OperatorCode] = adAggregate{
			LpHits:                 &counter{},
			LpMsisdnHits:           &counter{},
			MoTotal:                &counter{},
			MoChargeSuccess:        &counter{},
			MoChargeSum:            &counter{},
			MoChargeFailed:         &counter{},
			MoRejected:             &counter{},
			Outflow:                &counter{},
			RenewalTotal:           &counter{},
			RenewalChargeSuccess:   &counter{},
			RenewalChargeSum:       &counter{},
			RenewalFailed:          &counter{},
			InjectionTotal:         &counter{},
			InjectionChargeSuccess: &counter{},
			InjectionChargeSum:     &counter{},
			InjectionFailed:        &counter{},
			ExpiredTotal:           &counter{},
			ExpiredChargeSuccess:   &counter{},
			ExpiredChargeSum:       &counter{},
			ExpiredFailed:          &counter{},
			Pixels:                 &counter{},
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

	as.adReport[r.CampaignCode][r.OperatorCode].LpHits.Inc()
	if r.Msisdn != "" {
		as.adReport[r.CampaignCode][r.OperatorCode].LpMsisdnHits.Inc()
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
		as.adReport[r.CampaignCode][r.OperatorCode].MoTotal.Inc()
		if r.TransactionResult == "paid" {
			as.adReport[r.CampaignCode][r.OperatorCode].MoChargeSuccess.Inc()
			as.adReport[r.CampaignCode][r.OperatorCode].MoChargeSum.Add(r.Price)
		}
		if r.TransactionResult == "rejected" {
			as.adReport[r.CampaignCode][r.OperatorCode].MoRejected.Inc()
		}
		if r.TransactionResult == "failed" {
			as.adReport[r.CampaignCode][r.OperatorCode].MoChargeFailed.Inc()
		}
		log.WithField("tid", r.Tid).Debug("mo")
		return nil
	}

	if strings.Contains(r.TransactionResult, "retry") {
		as.adReport[r.CampaignCode][r.OperatorCode].RenewalTotal.Inc()

		if strings.Contains(r.TransactionResult, "retry_paid") {
			as.adReport[r.CampaignCode][r.OperatorCode].RenewalChargeSuccess.Inc()
			as.adReport[r.CampaignCode][r.OperatorCode].RenewalChargeSum.Add(r.Price)
		}

		if strings.Contains(r.TransactionResult, "retry_failed") {
			as.adReport[r.CampaignCode][r.OperatorCode].RenewalFailed.Inc()
		}
		log.WithField("tid", r.Tid).Debug("retry")
		return nil
	}

	if strings.Contains(r.TransactionResult, "injection") {
		as.adReport[r.CampaignCode][r.OperatorCode].InjectionTotal.Inc()

		if strings.Contains(r.TransactionResult, "injection_paid") {
			as.adReport[r.CampaignCode][r.OperatorCode].InjectionChargeSuccess.Inc()
			as.adReport[r.CampaignCode][r.OperatorCode].InjectionChargeSum.Add(r.Price)
		}

		if strings.Contains(r.TransactionResult, "injection_failed") {
			as.adReport[r.CampaignCode][r.OperatorCode].InjectionFailed.Inc()
		}
		log.WithField("tid", r.Tid).Debug("injection")
		return nil
	}

	if strings.Contains(r.TransactionResult, "expired") {
		as.adReport[r.CampaignCode][r.OperatorCode].ExpiredTotal.Inc()

		if strings.Contains(r.TransactionResult, "expired_paid") {
			as.adReport[r.CampaignCode][r.OperatorCode].ExpiredChargeSuccess.Inc()
			as.adReport[r.CampaignCode][r.OperatorCode].ExpiredChargeSum.Add(r.Price)
		}

		if strings.Contains(r.TransactionResult, "expired_failed") {
			as.adReport[r.CampaignCode][r.OperatorCode].ExpiredFailed.Inc()
		}
		log.WithField("tid", r.Tid).Debug("expired")
		return nil
	}

	return nil
}

func (as *collectorService) IncOutflow(r Collect) error {
	if err := as.check(r); err != nil {
		return err
	}
	as.Lock()
	defer as.Unlock()

	if strings.Contains(r.TransactionResult, "inact") ||
		strings.Contains(r.TransactionResult, "purge") ||
		strings.Contains(r.TransactionResult, "cancel") {
		log.WithField("tid", r.Tid).Debug("outflow")
		as.adReport[r.CampaignCode][r.OperatorCode].Outflow.Inc()
	}
	return nil
}

func (as *collectorService) IncPixel(r Collect) error {
	if err := as.check(r); err != nil {
		return err
	}
	as.Lock()
	defer as.Unlock()
	log.WithField("tid", r.Tid).Debug("pixel")
	as.adReport[r.CampaignCode][r.OperatorCode].Pixels.Inc()
	return nil
}
