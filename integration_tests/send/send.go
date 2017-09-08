package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/sumatra/analytics/kafkaprovider"
	"github.com/sumatra/analytics/messagebuffer"
)

// CONFIG : config
var CONFIG = "/opt/comcast/kafka_test/src/github.com/sumatra/kafka/send/config.json"

func main() {

	iterations := flag.Int("i", 100, "Iterations")
	topicS := flag.String("t", "test", "Topics")
	waitMs := flag.Int("w", 900, "Delay ms (0)")
	config := flag.String("c", CONFIG, "Config File")
	help := flag.Bool("h", false, "help")

	flag.Parse()
	if *help {
		fmt.Println(`
         send -i <iter> -t <topic> -c <config> -w <wait>
             -i <iterations>: iterations (10)
             -t <topic>  : Topic (test)
						 -c <config> : Config file for messagebuffer
						 -w <secs>   : Wait between iterations (0)
         `)
		os.Exit(0)
	}

	khost := os.Getenv("KHOST")
	if khost == "" {
		khost = "localhost"
	}

	kprovider, err := kafkaprovider.NewProvider(khost)
	if err != nil {
		fmt.Println("Cannot create Provider")
		panic(err)
	}
	buffer, err := messagebuffer.NewBuffer(kprovider, *config) // one MB buffer
	if err != nil {
		fmt.Println("Cannot create  Buffer")
		panic(err)
	}

	defer func() {
		if err := buffer.Close(); err != nil {
			panic(err)
		}
	}()
	start := time.Now().UnixNano() / int64(time.Millisecond)

	pid := os.Getpid()
	var x int
	for x = 1; x <= *iterations; x++ {
		if *waitMs > 0 {
			time.Sleep(time.Duration(*waitMs) * time.Millisecond)
		}
		fmt.Printf("%d", x)

		mess := strconv.Itoa(pid) + ": Something Cool-" + strconv.Itoa(x)
		err := buffer.WriteMessage(*topicS, mess, "key")

		if err != nil {
			fmt.Println(err)
		}
	}
	buffer.Close()
	var duration = time.Now().UnixNano()/int64(time.Millisecond) - start
	var rate = int64(x) / duration * 1000
	fmt.Printf("message sent %d, duration %d ms, rate=%d mess/s\n", x, duration, rate)

	fmt.Printf("\n")

}
