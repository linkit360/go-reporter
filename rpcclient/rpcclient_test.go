package rpcclient

import (
	"testing"

	log "github.com/Sirupsen/logrus"
	//"github.com/stretchr/testify/assert"
	//"github.com/vostrok/reporter/server/src/handlers"
)

func init() {
	c := ClientConfig{
		DSN:     "localhost:50313",
		Timeout: 10,
	}
	if err := Init(c); err != nil {
		log.WithField("error", err.Error()).Fatal("cannot init client")
	}
}

func Test(t *testing.T) {
}
