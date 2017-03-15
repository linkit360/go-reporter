package config

import (
	"flag"
	"fmt"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	"github.com/vostrok/acceptor/rpcclient"
	"github.com/vostrok/utils/db"
)

type ServerConfig struct {
	RPCPort  string `default:"50307" yaml:"rpc_port"`
	HttpPort string `default:"50308" yaml:"http_port"`
}
type AppConfig struct {
	AppName        string                 `yaml:"app_name"`
	Server         ServerConfig           `yaml:"server"`
	Collector      CollectorConfig        `yaml:"collector"`
	DbConf         db.DataBaseConfig      `yaml:"db"`
	AcceptorClient rpcclient.ClientConfig `yaml:"acceptor_client"`
}

type CollectorConfig struct {
	Provider string `yaml:"provider"`
}

func LoadConfig() AppConfig {
	cfg := flag.String("config", "dev/reporter.yml", "configuration yml file")
	flag.Parse()
	var appConfig AppConfig

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
			os.Exit(1)
		}
	}

	if appConfig.AppName == "" {
		log.Fatal("app name must be defiled as <host>-<name>")
	}

	if strings.Contains(appConfig.AppName, "-") {
		log.Fatal("app name must be without '-' : it's not a valid metric name")
	}

	appConfig.Server.RPCPort = envString("PORT", appConfig.Server.RPCPort)
	appConfig.Server.HttpPort = envString("METRICS_PORT", appConfig.Server.HttpPort)

	log.WithField("config", fmt.Sprintf("%#v", appConfig)).Info("Config loaded")
	return appConfig
}

func envString(env, fallback string) string {
	e := os.Getenv(env)
	if e == "" {
		return fallback
	}
	return e
}
