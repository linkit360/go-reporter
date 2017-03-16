package rpcclient

import (
	"testing"

	log "github.com/Sirupsen/logrus"

	"github.com/stretchr/testify/assert"
	"github.com/vostrok/reporter/server/src/collector"
)

func init() {
	c := ClientConfig{
		Enabled: true,
		DSN:     "localhost:50313",
		Timeout: 10,
	}
	if err := Init(c); err != nil {
		log.WithField("error", err.Error()).Fatal("cannot init client")
	}
}

func Test(t *testing.T) {
	r := collector.Collect{}
	err := IncMO(r)
	assert.NoError(t, err, "IncMO")

	err = IncPixel(r)
	assert.NoError(t, err, "IncPixel")

	err = IncHit(r)
	assert.Error(t, err, "IncHit")

	err = IncPaid(r)
	assert.NoError(t, err, "IncPaid")
}
