package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
)

var LOCALCONNECT = "172.17.0.1:9092"

// send messages over multiple topics at the same time using goroutines

func main() {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.RequiredAcks = sarama.WaitForAll

	var waitMs int
	var reps int
	var messageBodySize int
	var khost string
	var topic string
	var help bool

	flag.StringVar(&khost, "k", LOCALCONNECT, "kafka server")
	flag.StringVar(&topic, "t", "test", "Topic")
	flag.IntVar(&reps, "r", 10, "# of messages")

	flag.IntVar(&waitMs, "w", 0, "Delay ms (0)")
	flag.BoolVar(&help, "h", false, "Help")

	flag.IntVar(&messageBodySize, "s", 10, "Message Body Size")
	flag.Parse()

	if help {
		fmt.Println("async_kafka3.go -k <host> -t <topic:test> -r <reps:10>")
		os.Exit(1)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	var start = time.Now()
	fmt.Println("host=", khost, "topic=", topic)

	sendMess(khost, topic, messageBodySize, waitMs, reps, config, signals)

	var duration = time.Since(start).Seconds()

	var rate = float64(reps) / duration
	fmt.Println("\nAsyncProducer2")
	fmt.Printf("message sent %d, duration %v, rate=%.2f mess/s\n", reps, duration, rate)
}

func sendMess(khost string, topic string, messageBodySize int,
	waitMs int, reps int, config *sarama.Config,
	signals chan os.Signal) {

	producer, err := sarama.NewAsyncProducer([]string{khost + ":9092"}, config)

	if err != nil {
		panic(err)
	}
	defer func() {
		producer.Close()
	}()

	var enqueued, errors int64
	//var mess = "Test is a test message"
ProducerLoop:
	for {
		if waitMs > 0 {
			time.Sleep(time.Duration(waitMs) * time.Millisecond)
		}

		messageBody := sarama.ByteEncoder(make([]byte, messageBodySize))
		select {
		case producer.Input() <- &sarama.ProducerMessage{
			Topic: topic, Key: nil,
			Value: messageBody}: // sarama.StringEncoder(mess)}:
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
		if enqueued >= int64(reps) {
			break ProducerLoop
		}
	}
	fmt.Println("Waiting for errors..")

	start2 := time.Now()
	select {
	case er := <-producer.Errors():
		errors++
		fmt.Printf("%s \n", er)
	case <-time.After(2000 * time.Millisecond):
		fmt.Println("2 seconds done.")
	}
	fmt.Println("waiting for error", time.Since(start2))

	fmt.Println("loop done, errors=", errors, "sent=", enqueued)
}
