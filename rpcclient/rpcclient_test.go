package rpcclient

import (
	"testing"

	log "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"

	"github.com/linkit360/go-reporter/server/src/collector"
)

func init() {
	c := ClientConfig{
		DSN:     "localhost:50315",
		Timeout: 10,
	}
	if err := Init(c); err != nil {
		log.WithField("error", err.Error()).Fatal("cannot init client")
	}
}

func Test(t *testing.T) {
	r := collector.Collect{
		CampaignId:        1,
		OperatorCode:      41001,
		Msisdn:            "1234",
		TransactionResult: "paid",
	}
	err := IncMO(r)
	assert.NoError(t, err, "IncMO")

	err = IncPixel(r)
	assert.NoError(t, err, "IncPixel")

	err = IncHit(r)
	assert.NoError(t, err, "IncHit")

	err = IncTransaction(r)
	assert.NoError(t, err, "IncPaid")
}
