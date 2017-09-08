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

	var iterations int
	var waitMs int
	var topicS string
	var config string
	var help bool
	var providerPace int
	flag.IntVar(&iterations, "i", 100, "Iterations")
	flag.StringVar(&topicS, "t", "test", "Topics")
	flag.IntVar(&waitMs, "w", 900, "Delay ms (0)")
	flag.StringVar(&config, "c", CONFIG, "Config File")
	flag.BoolVar(&help, "h", false, "help")
	flag.IntVar(&providerPace, "p", 0, "provider pace")

	flag.Parse()
	if help {
		fmt.Println(`
         send -i <iter> -t <topic> -c <config> -w <wait> -p <pace>
             -i <iterations>: iterations (10)
             -t <topic>  : Topic (test)
						 -c <config> : Config file for messagebuffer
						 -w <secs>   : Wait between iterations (0)
						 -p <msecs>  : pace of provider (millsec)
         `)
		os.Exit(0)
	}

	khost := os.Getenv("KHOST")
	if khost == "" {
		khost = "localhost"
	}

	kprovider, err := kafkaprovider.NewProvider(khost)
	if providerPace > 0 {
		kprovider.SetPace(providerPace)
	}
	if err != nil {
		fmt.Println("Cannot create Provider")
		panic(err)
	}
	buffer, err := messagebuffer.NewBuffer(kprovider, config) // one MB buffer
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
	for x = 1; x <= iterations; x++ {
		if waitMs > 0 {
			time.Sleep(time.Duration(waitMs) * time.Millisecond)
		}
		fmt.Printf("%d", x)

		mess := strconv.Itoa(pid) + ": Something Cool-" + strconv.Itoa(x)
		err := buffer.WriteMessage(topicS, mess, "key")

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
