package rpcclient

import (
	"fmt"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/linkit360/go-reporter/server/src/collector"
	"github.com/linkit360/go-reporter/server/src/handlers"
	m "github.com/linkit360/go-utils/metrics"
)

var cli *Client

type Client struct {
	connection *rpc.Client
	conf       ClientConfig
	m          *Metrics
}

type ClientConfig struct {
	Timeout int    `default:"10" yaml:"timeout"`
	DSN     string `default:":50307" yaml:"dsn"`
}

type Metrics struct {
	RPCConnectError m.Gauge
	RPCSuccess      m.Gauge
}

func initMetrics() *Metrics {
	metrics := &Metrics{
		RPCConnectError: m.NewGauge("rpc", "reporter", "errors", "RPC call errors"),
		RPCSuccess:      m.NewGauge("rpc", "reporter", "success", "RPC call success"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			metrics.RPCConnectError.Update()
			metrics.RPCSuccess.Update()
		}
	}()
	return metrics
}
func Init(clientConf ClientConfig) error {
	var err error
	cli = &Client{
		conf: clientConf,
		m:    initMetrics(),
	}
	if err = cli.dial(); err != nil {
		err = fmt.Errorf("cli.dial: %s", err.Error())
		log.WithField("error", err.Error()).Error("reporter rpc client unavialable")
		return err
	}
	log.WithField("conf", fmt.Sprintf("%#v", clientConf)).Info("reporter rpc client init done")

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

func IncMO(req collector.Collect) error {
	var res handlers.Response
	err := call(
		"Collect.IncMO",
		req,
		&res,
	)
	return err
}

func IncPixel(req collector.Collect) error {
	var res handlers.Response
	err := call(
		"Collect.IncPixel",
		req,
		&res,
	)
	return err
}
func IncHit(req collector.Collect) error {
	var res handlers.Response
	err := call(
		"Collect.IncHit",
		req,
		&res,
	)
	return err
}
func IncPaid(req collector.Collect) error {
	var res handlers.Response
	err := call(
		"Collect.IncPaid",
		req,
		&res,
	)
	return err
}
