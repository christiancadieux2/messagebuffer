package kafkaprovider

import (
	"log"
	"strings"
	"util"

	"github.com/Shopify/sarama"
	"github.com/twinj/uuid"
)

const defaultPort = "9092"

// speed tests:
//   sarama-sync  :   15K mess/sec
//   sarama-async :  450K mess/s
//   confluent 'C':   45K mess/s
//   file-buffer  :  1200K message/sec
// KafkaProvider manage one sarama configuration and one current producer.
type KafkaProvider struct {
	hosts         string
	brokers       []string
	config        *sarama.Config
	producer      sarama.AsyncProducer
	retryWaitTime int
	clientID      string
}

// NewProvider creates a kafkaProvider
func NewProvider(hosts string, retryWait int, clientId string) (*KafkaProvider, error) {

	kc := new(KafkaProvider)

	brokers := strings.Split(hosts, ",")
	for i := 0; i < len(brokers); i++ {
		if strings.Index(brokers[i], ":") < 0 {
			brokers[i] = brokers[i] + ":" + defaultPort
		}
	}
	kc.brokers = brokers
	kc.hosts = hosts

	kc.retryWaitTime = retryWait

	util.Logln("Creating kafka handle", brokers)

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 4
	config.Producer.Return.Successes = false

	config.Producer.Compression = sarama.CompressionSnappy
	if clientId == "" {
		clientId = "golang-headwaters-sample-producer"
	}
	config.ClientID = clientId
	// config.Producer.Flush.Frequency = 500 * time.Millisecond
	// config.ChannelBufferSize = 10000

	config.Producer.Partitioner = sarama.NewHashPartitioner

	kc.config = config
	return kc, nil
}

// Name of provider
func (kc *KafkaProvider) Name() string {
	return "Kafka at " + kc.hosts
}

// GetRetryWaitTime informs the messagebuffer how long to wait when kafka is down
// when kafka restarts for example, sarama will fail and succeed a few time before returning
// consistent errors so it's best to stop trying. Also sarama has it's own maxretry.
func (kc *KafkaProvider) GetRetryWaitTime() int {
	return kc.retryWaitTime
}

//OpenProducer creates an async producer.
func (kc *KafkaProvider) OpenProducer() error {

	producer, err := sarama.NewAsyncProducer(kc.brokers, kc.config)
	if err != nil {
		kc.producer = nil
		return err
	} else {
		kc.producer = producer
		return nil
	}
}

// CloseProducer close the producer.
func (kc *KafkaProvider) CloseProducer() error {
	return kc.producer.Close()
}

// SendMessage send a message and listen for errors
//strTime := strconv.Itoa(int(time.Now().Unix()))
//Key:   sarama.StringEncoder(strTime), Partition: 6
// Not setting a message key means that all messages will
//  be distributed randomly over the different partitions.

func (kc *KafkaProvider) SendMessage(topic string, mess string, key string) (int, error) {

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(mess),
	}

	if key == "" {
		eventId := uuid.Formatter(uuid.NewV4(), uuid.FormatCanonical)
		msg.Key = sarama.StringEncoder(eventId)
	} else {
		msg.Key = sarama.StringEncoder(key)
	}

	sent := 0
	var lastError error

	select {
	case kc.producer.Input() <- msg:
		sent++
	case err := <-kc.producer.Errors():
		log.Println("Failed to produce message", err)
		lastError = err
	}
	return sent, lastError
}
