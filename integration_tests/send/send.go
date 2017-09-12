package main

import (
	"clog"
	"context"
	"fakeprovider"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	"util"

	"kafkaprovider"
	"messagebuffer"

	"github.com/gin-gonic/gin"
)

// CONFIG : config
var CONFIG = "/opt/comcast/messagebuffer/integration_tests/send/config.json"

var default_topic = "raw.viper.sumatra.collector.LogEvent"
var inputDelay int
var webPort = "8080"

func main() {

	var iterations int
	var allDone bool = false
	var topicS string
	var config string
	var fakeProvider bool
	var help bool
	var outputDelay int
	var messageLen int
	flag.IntVar(&iterations, "i", 100, "Iterations")
	flag.StringVar(&topicS, "t", "test", "Topics")

	flag.IntVar(&messageLen, "l", 100, "Message Len (100)")
	flag.StringVar(&config, "c", CONFIG, "Config File")
	flag.BoolVar(&help, "h", false, "help")

	flag.IntVar(&inputDelay, "w", 1000, "Input delay microsec (1000)")
	flag.IntVar(&outputDelay, "p", 1000, "output delay microsec (1000) ")

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
						 -w <micros>   : Input Delay (1000 microsecs)
						 -p <micro>  : Output Delay (1000 microsecs)
         `)
		os.Exit(0)
	}

	khost := os.Getenv("KHOST")
	if khost == "" {
		khost = "localhost"
	}

	var err error
	var kprovider messagebuffer.Provider

	logger := clog.MakeLogger(clog.Fields{"s": "messagebuffer"})

	if fakeProvider {
		kprovider, err = fakeprovider.NewProvider(khost, 0)
	} else {
		kprovider, err = kafkaprovider.NewProvider(khost, 10, "")
	}

	if err != nil {
		fmt.Println("Cannot create Provider")
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	buffer, err := messagebuffer.NewBuffer(ctx, kprovider, config, logger) // one MB buffer
	if outputDelay > 0 {
		buffer.SetOutputDelay(outputDelay)
	}
	if err != nil {
		fmt.Println("Cannot create  Buffer")
		panic(err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		util.Logln(sig)
		allDone = true
		cancel() // <- ctx.Done()
	}()

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

	for x = 1; x <= iterations && !allDone; x++ {
		if inputDelay > 0 {
			time.Sleep(time.Duration(inputDelay) * time.Microsecond)
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
	fmt.Println("/injectError : Inject Provider error.")
	fmt.Println("/inputDelay/<microsecs> : delay when writing to buffer.")
	fmt.Println("/optputDelay/<microsecs> : delay when writing to kafka.")
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	r.GET("/injectError", func(c *gin.Context) {
		fmt.Println("Inject Error")
		buffer.InjectError()
		c.String(http.StatusOK, "OK")
	})
	r.GET("/inputDelay/:delay", func(c *gin.Context) {
		delay := c.Param("delay")
		delay_micros, err := strconv.Atoi(delay)
		if err != nil {
			c.String(http.StatusBadRequest, "Invalid input delay (microsec)="+delay)
		} else {
			inputDelay = delay_micros
			c.String(http.StatusOK, "OK")
		}
	})
	r.GET("/outputDelay/:delay", func(c *gin.Context) {
		delay := c.Param("delay")
		delay_micros, err := strconv.Atoi(delay)
		if err != nil {
			c.String(http.StatusBadRequest, "Invalid output delay (microsec)="+delay)
		} else {
			buffer.SetOutputDelay(delay_micros)
			c.String(http.StatusOK, "OK")
		}
	})

	r.Run(":" + webPort)
}

func speed(count int, start time.Time, prefix string) {
	lapse := time.Since(start)
	rate := int64(count) / int64(lapse/time.Second)
	fmt.Println(prefix, "messages sent:", count, ", duration:", lapse,
		", speed:", rate, "mess/sec")
}
