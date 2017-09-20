# messagebuffer

This library is used to 'buffer' messages between a client and providers like kafka.

Main features are:

 - Messages are sent 'live' to provider until the first error, where buffering starts.
 - Buffering saved the messages in local files (message buffers).

 - Message files are limited in age and size so that the delay before messages are sent to the provider is small and constant.

 - A separate thread is responsible to send the messages in the same order to providers like kafka and deal with interruptions in the provider availability.

 - Buffering is useful when there is interruptions in network or provider itself. It is also useful client messages frequency is too high for the provider for (hopefully) short periods of time.

 - Message files are created every X seconds or every Y MB , whichever comes first.

 - In cases where the services need to be restarted or if kafka is down for too long, the current seek value of the message file being transmitted is saved in a corresponding 'S' file. This allows the service to resume where it was aborted and prevent repeating messages.


 - When the total size of the message files reach a maximum , oldest files are removed to make space for the new ones.

 - Messages currently include a topic and message. newlines in the message are encoded ('\\\n').

 - messagebuffer defines a 'Provider' inteface for use by providers. Example with a kafka Provider:

```
  kprovider, err := kafkaprovider.NewProvider(khost)
	buffer, err := messagebuffer.NewBuffer(kprovider, *config) // one MB buffer   
  err := buffer.WriteMessage(*topicS, mess, "key")
  ..
  buffer.Close()
```

Reseach:

I considered these different approaches:

  - calling kafka directly: client need to deal with kafka availability, errors etc..
    If client is too fast or kafka is too slow, client can block.

  - using io.stream: buffering is done in memory, not big enough.

  - using os.pipes: can use a file for buffering but both sides must be connected to the file and it's hard to trim from the beginning of the pipe when it gets too big.

Using multiple small files seemed like a good solution, easy to prune, easy to seek in the file. writing is fast as long as the buffer files are local. If the files are 10seconds file for example, priming the buffer is fast.
It's also possible if needed to send messages to kafka directly until the first error and then switch to 'buffering'.
Most of the time, there should be very few message files present unless the provider cannot keep up with the client (spikes).

Background:

Modern disk-drives can do 100MB/second / (100 bytes messages) is 1M mess/s.
1Gigabit network under ideal conditions can do 1G / (8 bit/byte) / (100 bytes message) = 1.25M mess/s.




Speed Tests: Done on ubuntu/corei7 3.6Ghz with kafka single node running locally in container.
```
   sarama-sync  :   15K mess/sec
   sarama-async :  450K mess/s
   confluent 'C':   45K mess/s
   file-buffer  :  1200K message/sec
```

The 'C' interface is faster than sarama-sync but sarama-async is much faster. There is probably a way to use the 'C'library in async mode but sarama has much better documentation and finding async examples was easier to I stayed with sarama. Also, must easier to install.

TODO:

 - Use separate kafka connector and goroutine for each kafka 'topic' or group of topics to improve performance. kafka for example scale by partition/topic.

 - Add an option to send messages directly to the provider and bypass the buffer files until the first provider error occur. This prevents the delay between message generation and messages available to the provider. At the firt error, switch to buffered mode so the client does not have to block or deal with errors. This could be a useful options for cases where getting the message to kafka in real-time if possible is needed.

 - use clog.


# kafkaprovider
The kafka provider uses the Sarama kafka library in async mode to send messages to kafka. It implements NewProvider, OpenProducer, SendMessage, CloseProducer and GetRetryWaitTime.
In async mode, sending messages to kafka is much faster but a 'select' need to be used to send to the Input() channel at the same time as listening on the Error() channel.

## Diagram

![Diagram](https://user-images.githubusercontent.com/10535265/30186693-d5669528-93e3-11e7-89b9-25bd269ac228.png)
-

## Simulation
![Diagram](https://user-images.githubusercontent.com/31523474/30551773-e631ab18-9c58-11e7-8206-7f2fccbe3afb.png)

## kafkaprovider
![kafkaprovider](https://user-images.githubusercontent.com/31523474/30622469-109f6536-9d6f-11e7-96b9-f8fc100ac2a3.png)


Jeff Dean: "Numbers everyone should know."
```
L1 cache reference                             0.5 ns   
Branch mispredict                              5 ns
L2 cache reference                             7 ns
Mutex lock/unlock                             25 ns
Main memory reference                        100 ns
Compress 1K bytes with Zippy               3,000 ns
Send 2K bytes over 1 Gbps network         20,000 ns
Read 1 MB sequentially from memory       250,000 ns
Round trip within same datacenter        500,000 ns
Disk seek                             10,000,000 ns
Read 1 MB sequentially from disk      20,000,000 ns
Send packet CA->Netherlands->CA      150,000,000 ns

*Write 1MB sequentially to disk       10,000,000 ns    1/100 second
*Write 1MB over 1GBps network         10,000,000 ns
*Compress 1M bytes with Zippy          3,000,000 ns


```
