package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

var connector = "172.17.0.1:9092"

// send messages over multiple topics at the same time using goroutines

func main() {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.RequiredAcks = sarama.WaitForAll

	waitMs := flag.Int("w", 0, "Delay ms (0)")
	reps := flag.Int("m", 10, "# of messages")
	gos := flag.Int("g", 1, "# of goroutines")
	flag.Parse()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup

	var start = time.Now().UnixNano() / int64(time.Millisecond)

	wg.Add(*gos)
	for i := 0; i < *gos; i++ {
		go sendMess("topic"+strconv.Itoa(i), waitMs, reps, config, signals, &wg)
	}

	fmt.Println("blocking:start")
	wg.Wait()
	fmt.Println("blocking:end")

	var duration = time.Now().UnixNano()/int64(time.Millisecond) - start
	enqueued := int64(*reps * *gos)
	var rate = enqueued / duration * 1000
	fmt.Println("\nAsyncProducer2")
	fmt.Printf("message sent %d, duration %d ms, rate=%d mess/s\n", enqueued, duration, rate)
	fmt.Println("\nasync_kafka2 -w <delay:0> -m <#messages:1000> -g<#goroutines(1)> ")
}

func sendMess(topic string, waitMs *int, reps *int, config *sarama.Config, signals chan os.Signal, wg *sync.WaitGroup) {

	producer, err := sarama.NewAsyncProducer([]string{connector}, config)
	fmt.Println("topic = ", topic)

	if err != nil {
		panic(err)
	}
	defer func() {
		producer.Close()
	}()

	var enqueued, errors int64
	var mess = "Test is a test message"
ProducerLoop:
	for {
		if *waitMs > 0 {
			time.Sleep(time.Duration(*waitMs) * time.Millisecond)
		}

		select {
		case producer.Input() <- &sarama.ProducerMessage{
			Topic: topic, Key: nil,
			Value: sarama.StringEncoder(mess)}:
			enqueued++
			//log.Println("enqueued", enqueued)
		case <-producer.Errors():
			// possible err.Err:
			//kafka: client has run out of available brokers to talk to (Is your cluster reachable?)
			//circuit breaker is open
			//log.Println("ERROR:", err.Err)
			errors++
		case <-signals:
			fmt.Println("Got signal")
			break ProducerLoop
		}
		if enqueued >= int64(*reps) {
			break ProducerLoop
		}
	}
	wg.Done()
	fmt.Println("loop done, errors=", errors, "sent=", enqueued)
}
