package main

import (
	"christiancadieux2/messagebuffer/pkg/kafkaprovider"
	"christiancadieux2/messagebuffer/pkg/messagebuffer"
	"christiancadieux2/messagebuffer/pkg/util"
	"context"
	"io"

	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"comcast-viper/clog"

	"github.com/gin-gonic/gin"
)

var defaultTopic = "raw.viper.sumatra.collector.LogEvent"
var inputDelay int
var webPort = "8080"
var currentInRate int64
var BASE string

func main() {

	var iterations int
	var allDone = false
	var topicS string
	var config string

	var help bool

	var outputDelay int
	var messageLen int
	var buffering bool
	BASE = os.Getenv("CONFIGBASE")
	if BASE == "" {
		BASE = "/Users/ccadie311/go/src/christiancadieux2/messagebuffer/"
	}
	var CONFIG = BASE + "/integration_tests/send/config.json"

	flag.IntVar(&iterations, "i", 100, "Iterations")
	flag.StringVar(&topicS, "t", "test", "Topics")

	flag.IntVar(&messageLen, "l", 100, "Message Len (100)")
	flag.StringVar(&config, "c", CONFIG, "Config File")
	flag.BoolVar(&help, "h", false, "help")
	flag.BoolVar(&buffering, "B", false, "buffering")

	flag.IntVar(&inputDelay, "w", 1000, "Input delay microsec (1000)")
	flag.IntVar(&outputDelay, "p", 1000, "output delay microsec (1000) ")

	flag.Parse()
	if help {
		fmt.Println(`
	KHOST="consumer-kafka.hdw.r53.deap.tv"
  send -i <iter> -t <topic> -c <config> -w <wait> -p <pace>
     -i <iterations>: iterations (10)

     -t <topic>  : Topic (test)
     -l <len>    : message length
     -c <config> : Config file for messagebuffer
     -w <micro>  : Input Delay (1000 microsecs)
     -p <micro>  : Output Delay (1000 microsecs)

		  go run integration_tests/send/mb_send.go  -i 0 -B

         `)
		os.Exit(0)
	}
	if iterations == 0 {
		iterations = 10000000000
	}

	khost := os.Getenv("KHOST")
	if khost == "" {
		khost = "localhost"
	}

	var err error
	var kprovider messagebuffer.Provider

	logger := clog.MakeLogger(clog.Fields{"s": "messagebuffer"})

	kprovider, err = kafkaprovider.NewProvider(khost, 15*time.Second, "")

	if err != nil {
		fmt.Println("Cannot create Provider")
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var mode = messagebuffer.ModeErrorOnError

	if buffering {
		mode = messagebuffer.ModeAlwaysBuffer

	}

	buffer, err := messagebuffer.NewBuffer(ctx, kprovider, config, logger, mode)

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
	mess0 := "This is a test message "
	mess := strconv.Itoa(pid) + ": " + mess0 + mess0 + mess0 + mess0 + "1"
	fmt.Println("Sending", iterations, "messages of ", len(mess), "bytes")
	fmt.Println("Input Delay:", inputDelay, "microsecs", "\n...")
	var lastx int
	for x = 1; x <= iterations && !allDone; x++ {
		select {
		case <-ctx.Done():
			break
		default:
		}
		if inputDelay > 0 {
			od := buffer.GetOutputDelay()
			if !buffering {
				max := inputDelay
				if od > inputDelay {
					max = od
				}
				time.Sleep(time.Duration(max) * time.Microsecond)
			} else {
				time.Sleep(time.Duration(inputDelay) * time.Microsecond)
			}
		}
		if time.Since(startMod) > time.Duration(1*time.Second) {
			speed(x-lastx, startMod, "")
			lastx = x
			startMod = time.Now()
		}

		err := buffer.WriteMessage(topicS, mess, "key")

		if err != nil {
			fmt.Println(err)
		}
	}
	x--
	buffer.Close()
	speed(x, start, "Total")

	time.Sleep(4 * time.Second)
	fmt.Printf("all done.\n")

}

// /injectError : Inject Provider error.")
// /inputDelay/<microsecs> : delay when writing to buffer.
// /optputDelay/<microsecs> : delay when writing to kafka
func server(buffer *messagebuffer.MessageBufferHandle) {

	gin.SetMode(gin.ReleaseMode)
	gin.DisableConsoleColor()
	f, _ := os.Create("gin.log")
	gin.DefaultWriter = io.MultiWriter(f)

	r := gin.Default()
	r.Static("/html", BASE+"/html")

	r.GET("/injectError", func(c *gin.Context) {
		fmt.Println("Inject Error")
		buffer.InjectError()
		c.String(http.StatusOK, "OK")
	})
	r.GET("/inputDelay/:delay", func(c *gin.Context) {
		delay := c.Param("delay")
		delayMicros, err := strconv.Atoi(delay)
		delayMicros = delayMicros * 1000
		if err != nil {
			c.String(http.StatusBadRequest, "Invalid input delay (microsec)="+delay)
		} else {
			inputDelay = delayMicros
			//fmt.Println("inputdelay_microsec=", delayMicros)
			c.String(http.StatusOK, "OK")
		}
	})
	r.GET("/outputDelay/:delay", func(c *gin.Context) {
		delay := c.Param("delay")
		delayMicros, err := strconv.Atoi(delay)

		if err != nil {
			c.String(http.StatusBadRequest, "Invalid output delay (microsec)="+delay)
		} else {
			v := delayMicros * 1000
			if delayMicros == 0 {
				v = 500
			}
			fmt.Println("outdelay=", delayMicros)
			buffer.SetOutputDelay(v)
			c.String(http.StatusOK, "OK")
		}
	})

	r.GET("/buffering/:val", func(c *gin.Context) {
		buff := c.Param("val")
		fmt.Println("buffering=", buff)
		if buff == "1" {
			buffer.SetErrorMode(messagebuffer.ModeAlwaysBuffer)
		} else {
			buffer.SetErrorMode(messagebuffer.ModeErrorOnError)
		}
		c.String(http.StatusOK, "OK")
	})

	r.GET("/info", func(c *gin.Context) {
		if buffer.GetErrorMode() == messagebuffer.ModeErrorOnError {
			c.String(http.StatusOK, "No Buffering")
		} else {
			c.String(http.StatusOK, "Buffering: "+buffer.GetConfig())
		}
	})

	r.GET("/speed", func(c *gin.Context) {

		out := fmt.Sprintf("%d,%d,%s",
			currentInRate, buffer.GetOutRate(), buffer.GetBufferList())
		//fmt.Println("speed=", out)
		c.String(http.StatusOK, out)
	})

	r.Run(":" + webPort)
}

func speed(count int, start time.Time, prefix string) {

	lapse2 := time.Since(start)

	rate := float64(count) / lapse2.Seconds()
	currentInRate = int64(rate)
	//fmt.Printf("%s message sent: %d, duration: %v , rate: %.3f mess/s \n",
	//	prefix, count, lapse2, rate)

}
