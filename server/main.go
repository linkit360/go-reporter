package main

import (
	"os"
	"os/signal"
	"syscall"

	reporter_server "github.com/linkit360/go-reporter/server/src"
)

func main() {
	c := make(chan os.Signal, 3)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		reporter_server.OnExit()
		os.Exit(1)
	}()

	reporter_server.Run()
}
