package util

import (
	"fmt"
	"time"
)

func Logln(args ...interface{}) {
	fmt.Println(args...)
}

func Speed(count int64, start time.Time, prefix string) string {
	lapse := int64(time.Since(start)) // in nanosec
	//fmt.Println("lapse=", lapse)
	rate := count * 1000000000 / lapse
	return fmt.Sprintf(prefix+": messages sent: %d, duration: %v ns, speed:%d mess/sec",
		count, lapse, rate)

}
