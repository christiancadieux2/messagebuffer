#!/bin/sh


go run async_kafka3.go  -k consumer-kafka.hdw.r53.deap.tv -t raw.mirrored.xre.x1.events -r 200000

message sent 200000, duration 2.22665991, rate=89820.63 mess/s

# test invalid event: returns error within 1 second
go run async_kafka3.go  -k consumer-kafka.hdw.r53.deap.tv -t raw.mirrored.xre.x1.events2 -r 200000

