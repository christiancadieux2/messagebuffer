package messagebuffer

// Providers are used by messagebuffer to talk to 'Producers' like kafka
// multiple threads can SendMessage using same Provider if producer support it.
// could add provider.mutex to SendMessage when Producer is not thread-safe
// NewProvider : create/configure/test new Producer
// OpenProducer => SendMessage(s) => CloseProducer
// GetRetryWaitTime: how long to wait between retries on Producer error.
// SendMessage(): returns #mess, #errors. If async mode (kafka), errors can be
//                from previous messages.
// setPace(int) : number of microsec to wait before calling sendMessage,
//                used for testing
type Provider interface {
	OpenProducer() error
	SendMessage(string, string) (int, error)
	CloseProducer() error
	GetRetryWaitTime() int
	Name() string
	SetPace(int)
}
