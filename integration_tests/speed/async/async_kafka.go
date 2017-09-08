package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

func main() {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll

	producer, err := sarama.NewAsyncProducer([]string{"localhost:9092"}, config)

	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var (
		wg                sync.WaitGroup
		successes, errors int
	)
	var enqueued int64

	wg.Add(1)
	go func() {
		defer wg.Done()
		for range producer.Successes() {
			//log.Println("success", successes)
			successes++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Println(err)
			errors++
		}
	}()

	var cnt int
	var start = time.Now().UnixNano() / int64(time.Millisecond)
ProducerLoop:
	for {
		message := &sarama.ProducerMessage{Topic: "speed", Value: sarama.StringEncoder("testing 123")}

		select {
		case producer.Input() <- message:
			enqueued++
			//log.Println("send ", enqueued)

		case <-signals:
			producer.AsyncClose() // Trigger a shutdown of the producer.
			break ProducerLoop
		}
		if cnt++; cnt > 11000 {
			producer.AsyncClose()
			break ProducerLoop
		}
	}

	wg.Wait()

	if errors > 0 {
		log.Printf("Successfully produced: %d; errors: %d\n", successes, errors)
	}

	var duration = time.Now().UnixNano()/int64(time.Millisecond) - start
	var rate = enqueued / duration * 1000
	fmt.Println("AsyncProducer")
	fmt.Printf("message sent %d, duration %d ms, rate=%d mess/s\n", enqueued, duration, rate)

}
