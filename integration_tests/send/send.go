package main

import (
	"fakeprovider"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"kafkaprovider"
	"messagebuffer"

	"github.com/gin-gonic/gin"
)

// CONFIG : config
var CONFIG = "/opt/comcast/messagebuffer/integration_tests/send/config.json"

func main() {

	var iterations int
	var waitMs int
	var topicS string
	var config string
	var fakeProvider bool
	var help bool
	var providerPace int
	var messageLen int
	flag.IntVar(&iterations, "i", 100, "Iterations")
	flag.StringVar(&topicS, "t", "test", "Topics")
	flag.IntVar(&waitMs, "w", 1000, "Delay microsec (1000)")
	flag.IntVar(&messageLen, "l", 100, "Message Len (100)")
	flag.StringVar(&config, "c", CONFIG, "Config File")
	flag.BoolVar(&help, "h", false, "help")
	flag.IntVar(&providerPace, "p", 0, "provider pace")
	flag.BoolVar(&fakeProvider, "f", false, "fake provider")

	flag.Parse()
	if help {
		fmt.Println(`
         send -i <iter> -t <topic> -c <config> -w <wait> -p <pace> -f
             -i <iterations>: iterations (10)
						 -f : use fake provider
             -t <topic>  : Topic (test)
						 -l <len>    : message length
						 -c <config> : Config file for messagebuffer
						 -w <micros>   : Wait between iterations (1000)
						 -p <msecs>  : pace of provider (millisec)
         `)
		os.Exit(0)
	}

	khost := os.Getenv("KHOST")
	if khost == "" {
		khost = "localhost"
	}

	var err error
	var kprovider messagebuffer.Provider

	if fakeProvider {
		kprovider, err = fakeprovider.NewProvider(khost, 0)
	} else {
		kprovider, err = kafkaprovider.NewProvider(khost, 10)
	}
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

	go server(buffer)

	defer func() {
		if err := buffer.Close(); err != nil {
			panic(err)
		}
	}()
	start := time.Now()
	startMod := time.Now()
	pid := os.Getpid()
	var x int

	for x = 1; x <= iterations; x++ {
		if waitMs > 0 {
			time.Sleep(time.Duration(waitMs) * time.Microsecond)
		}

		if x%1000 == 0 {
			speed(1000, startMod, "")
			startMod = time.Now()
		}
		mess0 := "This is a test message "
		mess := strconv.Itoa(pid) + ": " + mess0 + mess0 + mess0 + mess0 + strconv.Itoa(x)
		err := buffer.WriteMessage(topicS, mess, "key")

		if err != nil {
			fmt.Println(err)
		}
	}
	buffer.Close()
	speed(x, start, "Total")

	time.Sleep(4 * time.Second)
	fmt.Printf("all done.\n")

}

// inject error: wget localhost:8080/injectError
func server(buffer *messagebuffer.MessageBufferHandle) {
	fmt.Println("localhost:8080/injectError : Inject Provider error.")
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.GET("/injectError", func(c *gin.Context) {
		fmt.Println("Inject Error")
		buffer.InjectError()
		c.String(http.StatusOK, "OK")
	})

	r.Run(":8080")
}

func speed(count int, start time.Time, prefix string) {
	lapse := time.Since(start)
	rate := int64(count) / int64(lapse/time.Second)
	fmt.Println(prefix, "messages sent:", count, ", duration:", lapse,
		", speed:", rate, "mess/sec")
}
