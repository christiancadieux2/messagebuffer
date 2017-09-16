package clog

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/pborman/uuid"
	"golang.org/x/net/context"
)

const (
	// TraceHeader is the header under which http transactions
	// should store there distributed trace info.
	TraceHeader = "X-MoneyTrace"
)

// TraceContext with a request tracer.
func TraceContext(ctx context.Context, t *Trace) context.Context {
	return context.WithValue(ctx, tracerKey, t)
}

// TraceCtxNamed adds a new trace to the context with the passed in
// name. If the context contains a trace this is a subtrace, if it
// does not this is a newtrace.
func TraceCtxNamed(ctx context.Context, name string) context.Context {
	t, foundT := CtxTrace(ctx)
	if !foundT {
		t = NewTrace(name)
	} else {
		t = NewSubTrace(t, name)
	}
	return TraceContext(ctx, t)
}

// CtxTrace returns the trace for the context or nil and false if
// not present,
func CtxTrace(ctx context.Context) (*Trace, bool) {
	t, ok := ctx.Value(tracerKey).(*Trace)
	return t, ok
}

// A Trace for distributed requests.
type Trace struct {
	TraceID          string
	SpanID           int64
	SpanName         string
	ParentID         int64
	Start            time.Time
	Duration         time.Duration
	ResponseDuration time.Duration
	StatusDuration   time.Duration
	Err              error
	HTTPStatus       int
}

// NewTraceFromHeaders of a HTTP request.
func NewTraceFromHeaders(hdr string, name string) (*Trace, error) {
	t, p, s, err := ParseTraceHeaders(hdr)
	if err != nil {
		return nil, err
	}
	return &Trace{
		TraceID:  t,
		SpanID:   s,
		SpanName: name,
		ParentID: p,
		Start:    time.Now(),
	}, nil
}

// NewTrace with a UUID for TraceID.
func NewTrace(name string) *Trace {
	return &Trace{
		TraceID:  uuid.New(),
		SpanID:   rand.Int63(),
		SpanName: name,
	}
}

// NewSubTrace with t as a parent named name.
func NewSubTrace(t *Trace, name string) *Trace {
	return &Trace{
		TraceID:  t.TraceID,
		SpanID:   rand.Int63(),
		ParentID: t.SpanID,
		SpanName: name,
	}

}

// Started returns whether StartTrace has been called on this trace
// (aka t.Start is zero).
func (t *Trace) Started() bool {
	return !t.Start.IsZero()
}

// StartTrace set the start time of t if it has not already been
// started.
func (t *Trace) StartTrace() *Trace {
	if t.Started() {
		return t
	}
	newT := *t
	newT.Start = time.Now()
	return &newT
}

// EndTrace and calculate its duration. Set the err state to err.
func (t *Trace) EndTrace(err error) *Trace {
	if t.Duration != 0 || t.Err != nil {
		return t
	}
	newT := *t
	newT.Duration = time.Since(t.Start)
	newT.Err = err
	return &newT
}

// EndHTTPTrace set the total response-duration for an http
// transaction. This should include reading the body of the response.
func (t *Trace) EndHTTPTrace() *Trace {
	if t.ResponseDuration != 0 {
		return t
	}
	newT := *t
	newT.ResponseDuration = time.Since(newT.Start)
	return &newT
}

// SetHTTPStatus to status code and log the time it to to get the HTTP
// status response (used to diff from body response time).
func (t *Trace) SetHTTPStatus(status int) *Trace {
	if t.StatusDuration != 0 {
		return t
	}
	newT := *t
	newT.StatusDuration = time.Since(t.Start)
	newT.HTTPStatus = status
	if status >= 400 && newT.Err == nil {
		newT.Err = errors.New("HTTP Error")
	}
	return &newT
}

// ParseTraceHeaders values stored in the TraceHeader.
func ParseTraceHeaders(header string) (traceID string, parentID, spanID int64, err error) {

	values := strings.Split(header, ";")
	for _, s := range values {
		s = strings.Trim(s, " ")
		switch {
		case strings.HasPrefix(s, "trace-id="):
			traceID = strings.TrimPrefix(s, "trace-id=")
		case strings.HasPrefix(s, "span-id="):
			spanID, err = strconv.ParseInt(strings.TrimPrefix(s, "span-id="), 10, 64)
		case strings.HasPrefix(s, "parent-id="):
			parentID, err = strconv.ParseInt(strings.TrimPrefix(s, "parent-id="), 10, 64)
		}
		if err != nil {
			return // implied
		}
	}
	if traceID == "" || spanID == 0 {
		err = errors.New("Incomplete trace headers")
	}
	return // implied
}

// TraceHeaders translates the trace to HTTP headers.
func TraceHeaders(t *Trace) string {
	return fmt.Sprintf("trace-id=%s; parent-id=%d; span-id=%d", t.TraceID, t.ParentID, t.SpanID)
}

// A HTTPClient with context for performing requests.
type HTTPClient interface {
	Do(ctx context.Context, req *http.Request) (*http.Response, error)
}

// A TraceClient uses a ctx clog logger and a ctx trace if available
// to add a trace span to this request.
type TraceClient struct {
	c            HTTPClient
	l            Level
	timeResponse bool
}

// NewTraceClient wraps and HTTPClient with and logs trace info
// at level l.
func NewTraceClient(c HTTPClient, l Level) *TraceClient {
	return &TraceClient{c: c, l: l}
}

// NewFullTraceClient wraps and HTTPClient with and logs trace info at
// level l. In addition it logs the time taken for the response body
// to return io.EOF as response-duration in micros.
func NewFullTraceClient(c HTTPClient, l Level) *TraceClient {
	return &TraceClient{c: c, l: l, timeResponse: true}
}

// Do http request with context. If the ctx contains a clog logger and
// trace, trace info will be used and printed. In addition the trace
// header will be added.
// Traces should be placed in context like so:
// ctx = NewTraceCtxNamed(ctx, "Something")
// resp, err := tc.Do(ctx, req)
func (tc *TraceClient) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	l, foundL := CtxLogger(ctx)
	t, foundT := CtxTrace(ctx)
	// We need a logger and a trace that has _not_ been started
	// (else we might be using someone elses trace).
	if !foundL || !foundT || t.Started() {
		return tc.c.Do(ctx, req)
	}
	t = t.StartTrace()

	req.Header.Set(TraceHeader, TraceHeaders(t))
	resp, err := tc.c.Do(ctx, req)
	// In the event of an error or if we are not timing the http
	// response body trace ends here.
	if err != nil {
		t = t.EndTrace(err)
		tc.printTrace(t, req, l)
		return resp, err
	}

	t = t.SetHTTPStatus(resp.StatusCode)
	if !tc.timeResponse {
		t = t.EndTrace(err)
		tc.printTrace(t, req, l)
		return resp, err
	}

	resp.Body = &requestTrace{
		body: resp.Body,
		resp: resp,
		req:  req,
		tc:   tc,
		t:    t,
		l:    l,
	}

	return resp, err
}

func (tc *TraceClient) printTrace(t *Trace, req *http.Request, log *Logger) {
	log.TraceEntry(t).WithFields(Fields{
		"URL":    req.URL,
		"Method": req.Method,
	}).Printf(tc.l, "HTTP Client")
}

type requestTrace struct {
	body io.ReadCloser
	resp *http.Response
	req  *http.Request
	tc   *TraceClient
	t    *Trace
	l    *Logger
	done bool
}

func (rt *requestTrace) Read(p []byte) (n int, err error) {
	n, err = rt.body.Read(p)
	// Some callers hit us more than once, prevent us from logging
	// more than once.
	if !rt.done && err == io.EOF {
		t := rt.t.EndHTTPTrace().EndTrace(nil)
		rt.tc.printTrace(t, rt.req, rt.l)
		rt.done = true
	}
	return n, err
}

func (rt *requestTrace) Close() error {
	return rt.body.Close()
}

// A CtxHandler wraps an http endpoint taking some context. Returns an
// error if any to its handler if an http error will be returned to
// the client.
type CtxHandler interface {
	ServeHTTP(ctx context.Context, w http.ResponseWriter, req *http.Request) error
}

// A TraceHandler extracts any incoming money headers and adds our
// span to them. If no headers exist or can be parse the a new trace
// span and logger is placed in the ctx. If an error is returned this
// is logged as a span failure.
type TraceHandler struct {
	handler CtxHandler
	log     *Logger
	l       Level
	name    string
}

// Return a new tracehandler which invokes ctxhandler. The underlying
// logger is printed to at level l. The name of this span will be
// name.
func NewTraceHandler(handler CtxHandler, logger *Logger, l Level, name string) *TraceHandler {
	return &TraceHandler{
		handler: handler,
		log:     logger,
		l:       l,
		name:    name,
	}
}

// A CtxHandlerFunc takes a context and acts as an http.HandlerFunc.
type CtxHandlerFunc func(ctx context.Context, w http.ResponseWriter, req *http.Request) error

// The CtxHandlerWraper wraps a CtxHandlerFunc as a CtxHandler.
type CtxHandlerWraper struct {
	Func CtxHandlerFunc
}

// ServeHTTP invokeds the wrapped function with the arguments passed
// to it.
func (hw *CtxHandlerWraper) ServeHTTP(ctx context.Context, w http.ResponseWriter, req *http.Request) error {
	return hw.Func(ctx, w, req)
}

// NewTraceHandlerFunc wraps a CtxHandlerFunc in TraceHandler.
func NewTraceHandlerFunc(f CtxHandlerFunc, logger *Logger, l Level, name string) *TraceHandler {
	w := &CtxHandlerWraper{Func: f}
	return NewTraceHandler(w, logger, l, name)
}

// ServeHTTP for acting as a go webserver.
func (th *TraceHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var t *Trace
	var err error

	traceStr := req.Header.Get(TraceHeader)
	if traceStr == "" {
		th.log.NewEntry().WithFields(Fields{"TraceName": th.name}).Print(th.l, "No trace headers, generated new trace")
		t = NewTrace(th.name)
	} else {
		// Set the trace headers back on the response
		w.Header().Set(TraceHeader, traceStr)
		t, err = NewTraceFromHeaders(traceStr, th.name)
		if err != nil {
			th.log.NewEntry().WithFields(Fields{"TraceName": th.name, "error": err}).Print(th.l, "Could not parse trace headers, generated new trace")
			t = NewTrace(th.name)
		}
	}
	th.log.TraceEntry(t).WithFields(Fields{
		"URL":    req.URL,
		"Method": req.Method,
	}).Print(th.l, "HTTP Handler Request")

	t = NewSubTrace(t, th.name).StartTrace()
	ctx := NewLogContext(TraceContext(context.Background(), t), th.log)

	err = th.handler.ServeHTTP(ctx, w, req)
	// If our writer supports flushing do so, we want get accurate
	// timing info.
	if w, ok := w.(http.Flusher); ok {
		w.Flush()
	}

	t = t.EndTrace(err)
	th.log.TraceEntry(t).WithFields(Fields{
		"URL":    req.URL,
		"Method": req.Method,
	}).Print(th.l, "HTTP Handler Request Complete")
}

// TraceFunc trace any func() returning an error. Use of this method
// would look like:
//     ctx = clog.NewTraceCtxNamed(ctx, "DoSomething")
//     TraceFunc(ctx, clog.INFO, func() (err error) {
//         r, err = Something(v1, v2)
//	   return
//     })
// If a trace or logger is not found in the context f is invoked
// without tracing.
func TraceFunc(ctx context.Context, l Level, f func() error) error {
	log, foundL := CtxLogger(ctx)
	t, foundT := CtxTrace(ctx)
	if !foundT || !foundL || t.Started() {
		return f()
	}
	t = t.StartTrace()
	err := f()
	t = t.EndTrace(err)
	log.TraceEntry(t).Print(l)
	return err
}
