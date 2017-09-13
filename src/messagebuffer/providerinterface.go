package messagebuffer

// Providers are used by messagebuffer to talk to 'Producers' like kafka
// multiple threads can SendMessage using same Provider if producer support it.
// could add provider.mutex to SendMessage when Producer is not thread-safe
// NewProvider : create/configure/test new Producer
// OpenProducer => SendMessage(s) => CloseProducer
// GetRetryWaitTime: how long to wait between retries on Producer error.
// SendMessage(topic,message,key): returns #mess, error
//     key can be blank, topic are pre-defined

type Provider interface {
	OpenProducer() error
	SendMessage(string, string, string) (int, error)
	CloseProducer() error
	GetRetryWaitTime() int
	Name() string
}
