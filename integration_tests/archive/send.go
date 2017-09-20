package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	"github.com/Shopify/sarama"
)

func main() {

	iterations := flag.Int("i", 10, "Iterations")

	flag.Parse()

	localhost := os.Getenv("KHOST")
	if localhost == "" {
		localhost = "localhost"
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	// config.Producer.Flush.Frequency = 500 * time.Millisecond
	// config.ChannelBufferSize = 10000
	config.Producer.Return.Successes = true

	// brokers := []string{"192.168.59.103:9092"}
	brokers := []string{localhost + ":9092"}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		// Should not reach here
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			// Should not reach here
			panic(err)
		}
	}()

	topic := "raw.mirrored.xre.x1.events"
	pid := os.Getpid()
	for x := 1; x <= *iterations; x++ {
		fmt.Printf("%d", x)
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(strconv.Itoa(pid) + ": Something Cool-" + strconv.Itoa(x)),
		}
		//partition, offset, err := producer.SendMessage(msg)
		_, _, err := producer.SendMessage(msg)
		if err != nil {
			fmt.Println(err)
		}
	}

	if err != nil {
		panic(err)
	}
	fmt.Printf("\n")

	// fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
}
