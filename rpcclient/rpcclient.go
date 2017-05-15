package rpcclient

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/linkit360/go-reporter/server/src/collector"
	"github.com/linkit360/go-reporter/server/src/handlers"
	m "github.com/linkit360/go-utils/metrics"
)

var cli *Client

type Client struct {
	connection *rpc.Client
	conf       ClientConfig
	m          *Metrics
	ch         *ReserveCh
	stopping   bool
}

type ClientConfig struct {
	Timeout             int    `default:"10" yaml:"timeout"`
	DSN                 string `default:":50307" yaml:"dsn"`
	StateConfigFilePath string `yaml:"state_config_file_path"`
}

type SavedState struct {
	Hit         []collector.Collect `json:"hit"`
	Transaction []collector.Collect `json:"transaction"`
	Pixel       []collector.Collect `json:"pixel"`
	Outflow     []collector.Collect `json:"outflow"`
}

type ReserveCh struct {
	Hit         chan collector.Collect
	Transaction chan collector.Collect
	Pixel       chan collector.Collect
	Outflow     chan collector.Collect
}

type Metrics struct {
	RPCConnectError     m.Gauge
	RPCSuccess          m.Gauge
	HitChanSize         prometheus.Gauge
	OutflowChanSize     prometheus.Gauge
	PixelChanSize       prometheus.Gauge
	TransactionChanSize prometheus.Gauge
}

func initMetrics() *Metrics {
	metrics := &Metrics{
		RPCConnectError:     m.NewGauge("rpc", "reporter", "errors", "RPC call errors"),
		RPCSuccess:          m.NewGauge("rpc", "reporter", "success", "RPC call success"),
		HitChanSize:         m.PrometheusGauge("rpc", "reporter", "hit_chan_size", "hit chan size"),
		OutflowChanSize:     m.PrometheusGauge("rpc", "reporter", "outflow_chan_size", "outflow chan size"),
		PixelChanSize:       m.PrometheusGauge("rpc", "reporter", "pixel_chan_size", "pixel chan size"),
		TransactionChanSize: m.PrometheusGauge("rpc", "reporter", "transaction_chan_size", "transaction chan size"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			metrics.RPCConnectError.Update()
			metrics.RPCSuccess.Update()

			metrics.HitChanSize.Set(float64(len(cli.ch.Hit)))
			metrics.OutflowChanSize.Set(float64(len(cli.ch.Outflow)))
			metrics.PixelChanSize.Set(float64(len(cli.ch.Pixel)))
			metrics.TransactionChanSize.Set(float64(len(cli.ch.Transaction)))
		}
	}()
	return metrics
}

func Init(clientConf ClientConfig) error {
	var err error
	cli = &Client{
		conf: clientConf,
		m:    initMetrics(),
		ch: &ReserveCh{
			Pixel:       make(chan collector.Collect),
			Hit:         make(chan collector.Collect),
			Transaction: make(chan collector.Collect),
			Outflow:     make(chan collector.Collect),
		},
	}
	if err = cli.dial(); err != nil {
		err = fmt.Errorf("cli.dial: %s", err.Error())
		log.WithField("error", err.Error()).Error("reporter rpc client unavialable")
		return err
	}
	log.WithField("conf", fmt.Sprintf("%#v", clientConf)).Info("reporter rpc client init done")

	if err = loadState(); err != nil {
		log.WithField("error", err.Error()).Error("reporter load state")
	}

	go func() {
		for req := range cli.ch.Pixel {
			if cli.stopping {
				return
			}
			if err := incPixel(req); err != nil {
				cli.ch.Pixel <- req
				time.Sleep(time.Second)
			}
		}
	}()

	go func() {
		for req := range cli.ch.Hit {
			if cli.stopping {
				return
			}
			if err := incHit(req); err != nil {
				cli.ch.Hit <- req
				time.Sleep(time.Second)
			}
		}
	}()

	go func() {
		for req := range cli.ch.Transaction {
			if cli.stopping {
				return
			}
			if err := incTransaction(req); err != nil {
				cli.ch.Transaction <- req
				time.Sleep(time.Second)
			}
		}
	}()

	go func() {
		for req := range cli.ch.Outflow {
			if cli.stopping {
				return
			}
			if err := incOutflow(req); err != nil {
				cli.ch.Outflow <- req
				time.Sleep(time.Second)
			}
		}
	}()

	return nil
}

// on exit func
func SaveState() error {
	cli.stopping = true
	var state SavedState
	for req := range cli.ch.Outflow {
		state.Outflow = append(state.Outflow, req)
	}
	for req := range cli.ch.Hit {
		state.Hit = append(state.Hit, req)
	}
	for req := range cli.ch.Transaction {
		state.Transaction = append(state.Transaction, req)
	}
	for req := range cli.ch.Pixel {
		state.Pixel = append(state.Pixel, req)
	}
	stateJson, err := json.Marshal(state)
	if err != nil {
		err = fmt.Errorf("json.Marshal: %s", err.Error())
		return err
	}
	if err := ioutil.WriteFile(cli.conf.StateConfigFilePath, stateJson, 0644); err != nil {
		err = fmt.Errorf("ioutil.WriteFile: %s", err.Error())
		return err
	}
	log.Info("reporter save state done")
	return nil
}

func loadState() error {
	stateBytes, err := ioutil.ReadFile(cli.conf.StateConfigFilePath)
	if err != nil {
		err = fmt.Errorf("ioutil.ReadFile: %s", err.Error())
		return err
	}
	if len(stateBytes) == 0 {
		return nil
	}
	var state SavedState
	if err = json.Unmarshal(stateBytes, &state); err != nil {
		err = fmt.Errorf("json.Unmarshal: %s", err.Error())
		return err
	}
	for _, req := range state.Outflow {
		cli.ch.Outflow <- req
	}
	for _, req := range state.Hit {
		cli.ch.Hit <- req
	}
	for _, req := range state.Transaction {
		cli.ch.Transaction <- req
	}
	for _, req := range state.Pixel {
		cli.ch.Pixel <- req
	}
	log.Info("reporter load state done")
	return nil
}
func (c *Client) dial() error {
	if c.connection != nil {
		c.connection.Close()
		c.connection = nil
	}
	conn, err := net.DialTimeout(
		"tcp",
		c.conf.DSN,
		time.Duration(c.conf.Timeout)*time.Second,
	)
	if err != nil {
		log.WithFields(log.Fields{
			"dsn":   c.conf.DSN,
			"error": err.Error(),
		}).Error("dialing reporter")
		return err
	}
	c.connection = jsonrpc.NewClient(conn)
	return nil
}

func call(funcName string, req interface{}, res interface{}) error {
	begin := time.Now()

	retryCount := 0
retry:
	if err := cli.connection.Call(funcName, req, &res); err != nil {
		cli.m.RPCConnectError.Inc()

		if err == rpc.ErrShutdown {

			if retryCount < 2 {
				retryCount = retryCount + 1
				cli.connection.Close()
				cli.dial()
				log.WithFields(log.Fields{
					"retry": retryCount,
					"error": err.Error(),
				}).Debug("retrying..")
				goto retry
			}

			log.WithFields(log.Fields{
				"func":  funcName,
				"error": err.Error(),
			}).Fatal("call")
		}

		log.WithFields(log.Fields{
			"func":  funcName,
			"error": err.Error(),
			"type":  fmt.Sprintf("%T", err),
		}).Error("call")
		return err
	}
	log.WithFields(log.Fields{
		"func": funcName,
		"took": time.Since(begin),
	}).Debug("rpccall")
	cli.m.RPCSuccess.Inc()
	return nil
}

func IncPixel(req collector.Collect) {
	cli.ch.Pixel <- req
}

func incPixel(req collector.Collect) error {
	var res handlers.Response
	err := call(
		"Collect.IncPixel",
		req,
		&res,
	)
	return err
}

func IncHit(req collector.Collect) {
	cli.ch.Hit <- req
}
func incHit(req collector.Collect) error {
	var res handlers.Response
	err := call(
		"Collect.IncHit",
		req,
		&res,
	)
	return err
}

func IncTransaction(req collector.Collect) {
	cli.ch.Transaction <- req
}

func incTransaction(req collector.Collect) error {

	var res handlers.Response
	err := call(
		"Collect.IncTransaction",
		req,
		&res,
	)
	return err
}

func IncOutflow(req collector.Collect) {
	cli.ch.Outflow <- req
}

func incOutflow(req collector.Collect) error {
	var res handlers.Response
	err := call(
		"Collect.IncOutflow",
		req,
		&res,
	)
	return err
}
