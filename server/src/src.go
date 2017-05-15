package src

import (
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"runtime"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	"github.com/linkit360/go-reporter/server/src/config"
	"github.com/linkit360/go-reporter/server/src/handlers"
	m "github.com/linkit360/go-utils/metrics"
)

var appConfig config.AppConfig

func Run() {
	appConfig = config.LoadConfig()

	handlers.Init(appConfig)

	nuCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nuCPU)
	log.WithField("CPUCount", nuCPU)

	go runGin(appConfig)

	runRPC(appConfig)
}

func OnExit() {
	log.WithField("pid", os.Getpid()).Info("on exit")
	handlers.OnExit()
}

func runGin(appConfig config.AppConfig) {
	r := gin.New()
	m.AddHandler(r)

	r.Run(":" + appConfig.Server.HttpPort)

	log.WithField("port", appConfig.Server.HttpPort).Info("service port")

}

func runRPC(appConfig config.AppConfig) {

	l, err := net.Listen("tcp", "127.0.0.1:"+appConfig.Server.RPCPort)
	if err != nil {
		log.Fatal("netListen ", err.Error())
	}
	log.WithField("port", appConfig.Server.RPCPort).Info("rpc port")

	server := rpc.NewServer()
	server.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)
	server.RegisterName("Collect", &handlers.Collect{})

	for {
		if conn, err := l.Accept(); err == nil {
			go server.ServeCodec(jsonrpc.NewServerCodec(conn))
		} else {
			log.WithField("error", err.Error()).Error("accept")
		}
	}
}
