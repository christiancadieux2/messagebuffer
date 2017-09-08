package main

import (
	"fmt"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

func main() {
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		log.Fatalln(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	var count int64
	var start = time.Now().UnixNano() / int64(time.Millisecond)
	for {
		msg := &sarama.ProducerMessage{Topic: "speed",
			Value: sarama.StringEncoder("testing 123")}
		_, _, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("FAILED to send message: %s\n", err)
		} else {
			count++
			//log.Printf("message sent %d", count)
		}
		if count > 2000 {
			break
		}
	}
	var duration = time.Now().UnixNano()/int64(time.Millisecond) - start
	var rate = count / duration * 1000
	fmt.Println("SyncProducer")
	fmt.Printf("message sent %d, duration %d ms, rate=%d m/s\n", count, duration, rate)
}
