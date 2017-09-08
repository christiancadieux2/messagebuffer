package util

import (
	"fmt"
	"time"
)

func GetNow() int64 {
	return time.Now().Unix()
}
func Logln(args ...interface{}) {
	fmt.Println(args...)
}
