package util

import (
	"fmt"
	"os"
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
func FileSize(name string) int {
	fi, e := os.Stat(name)
	if e != nil {
		return 0
	}

	return int(fi.Size())
}

// ApendMax: Append until max then rotate
func AppendMax(v []string, val string, max int) []string {

	var v2 []string
	if len(v) < max {
		v2 = append(v, val)
	} else {
		v2 = append(v[1:], val)
	}

	return v2
}
