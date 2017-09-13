package fakeprovider

import (
	"time"
	"util"

	"github.com/Shopify/sarama"
)

const defaultPort = "9092"

// FakeProvider used to test buffers, does nothing
type FakeProvider struct {
	hosts         string
	brokers       []string
	config        *sarama.Config
	producer      sarama.AsyncProducer
	retryWaitTime time.Duration

	pace int // microsec
}

// NewProvider creates a kafkaProvider
func NewProvider(hosts string, retry time.Duration) (*FakeProvider, error) {

	kc := new(FakeProvider)

	kc.pace = 0
	util.Logln("Creating fake  handle")

	return kc, nil
}

// SetPace saves millisec to wait between calls to kafka
func (kc *FakeProvider) SetPace(s int) {
	kc.pace = s
}

// Name of provider
func (kc *FakeProvider) Name() string {
	return "Fake Provider"
}

func (kc *FakeProvider) GetRetryWaitTime() time.Duration {
	return 1 * time.Second
}

//OpenProducer creates an async producer.
func (kc *FakeProvider) OpenProducer() error {
	return nil
}

// CloseProducer close the producer.
func (kc *FakeProvider) CloseProducer() error {
	return nil
}

// SendMessage send a message and listen for errors
func (kc *FakeProvider) SendMessage(topic string, mess string, _ string) (int, error) {

	if kc.pace > 0 {
		time.Sleep(time.Duration(kc.pace) * time.Microsecond)
	}
	return 1, nil
}
