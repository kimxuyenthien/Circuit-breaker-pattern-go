package breaker

import (
	"errors"
	"strconv"
	"sync/atomic"
	"time"
)

type State int

type StateService int

const (
	STATE_CLOSE         State        = 1
	STATE_HALF_OPEN     State        = 2
	STATE_OPEN          State        = 3
	SERVICE_AVAILABLE   StateService = 1
	SERVICE_UNAVAILABLE StateService = 0
)

type ResponseCommand struct {
	Data  interface{}
	Error error
}

type HandleFunc func() ResponseCommand

type CounterResult struct {
	TotalRequests uint32
	TotalSucceses uint32
	TotalFailures uint32
	TotalRejects  uint32
}

type Breaker struct {
	options        OptionsConfig
	counter        CounterResult
	events         []chan BreakerEvent
	state          State
	consecFailures uint32
}

type BreakerEvent struct {
	Code    StateService
	Message string
}

type OptionsConfig struct {
	Attempts     uint32
	TimeoutState time.Duration
	LimitFailure uint32
	MaxRequests  uint32
	NameService  string
}

func (cb *Breaker) ErrorToManyRequest() error {
	return errors.New("To many requests")
}

func (cb *Breaker) ErrorTimeoutExecute() error {
	return errors.New("Execute is expired")
}

func (cb *Breaker) ErrorServiceUnavailable() error {
	return errors.New("Service is unavailable")
}

// create new instance breaker
func NewBreaker(options *OptionsConfig) *Breaker {

	if options == nil {
		options = &OptionsConfig{}
	}

	if options.Attempts == 0 {
		options.Attempts = 3
	}

	if options.TimeoutState == 0 {
		options.TimeoutState = 4 * time.Second
	}

	if options.LimitFailure == 0 {
		options.LimitFailure = 5
	}

	if options.MaxRequests == 0 {
		options.MaxRequests = 10000000
	}

	if options.NameService == "" {
		options.NameService = strconv.Itoa(int(time.Now().UnixNano()))
	}

	return &Breaker{options: *options, counter: CounterResult{}, state: STATE_CLOSE, consecFailures: 0}
}

// send event to other service
func (cb *Breaker) SendEvent(event *BreakerEvent) {

	for _, reader := range cb.events {
		reader <- *event
	}
}

// other services subcriber
func (cb *Breaker) Subcriber() <-chan BreakerEvent {

	evenReader := make(chan BreakerEvent)
	outputChannel := make(chan BreakerEvent, 100)

	go func() {
		for event := range evenReader {
			select {
			case outputChannel <- event:
			default:
				<-outputChannel
				outputChannel <- event
			}
		}
	}()

	cb.events = append(cb.events, evenReader)
	return outputChannel
}

func (cb *Breaker) SetState(newState State) {
	cb.state = newState
}

func (cb *Breaker) GetState() State {
	return cb.state
}

// check current state is OPEN
func (cb *Breaker) IsOpen() bool {
	if cb.GetState() == STATE_OPEN {
		return true
	}
	return false
}

// check current state is CLOSE
func (cb *Breaker) IsClose() bool {
	if cb.GetState() == STATE_CLOSE {
		return true
	}
	return false
}

// check current state is HALF_OPEN
func (cb *Breaker) IsHalfOpen() bool {
	if cb.GetState() == STATE_HALF_OPEN {
		return true
	}
	return false
}

// invoke is rejected
func (cb *Breaker) Reject() {
	atomic.AddUint32(&cb.counter.TotalRejects, 1)
}

// invoke is successful
func (cb *Breaker) Success() {
	atomic.AddUint32(&cb.counter.TotalSucceses, 1)
	if cb.IsHalfOpen() {

		// change state HALF_OPEN --> CLOSE
		cb.SetState(STATE_CLOSE)
		atomic.StoreUint32(&cb.consecFailures, 0)
	}
}

// invoke is failure
func (cb *Breaker) Failure() {

	atomic.AddUint32(&cb.counter.TotalFailures, 1)
	if cb.IsClose() {
		atomic.AddUint32(&cb.consecFailures, 1)
		consecFailures := atomic.LoadUint32(&cb.consecFailures)

		if consecFailures > cb.options.LimitFailure {
			// change state CLOSE --> OPEN
			cb.SetState(STATE_OPEN)
			cb.StartTimerChangeState()

			// send event service is unavailable to other proces
			event := &BreakerEvent{Code: SERVICE_UNAVAILABLE, Message: "Service " + cb.options.NameService + " is unavailable"}
			cb.SendEvent(event)
		}
	}
	if cb.IsHalfOpen() {

		// change state HALF_OPEN --> OPEN
		cb.SetState(STATE_OPEN)
		cb.StartTimerChangeState()

		// send event service is unavailable to other proces
		event := &BreakerEvent{Code: SERVICE_UNAVAILABLE, Message: "Service " + cb.options.NameService + " is unavailable"}
		cb.SendEvent(event)
	}
}

// start timer for change state service
func (cb *Breaker) StartTimerChangeState() {

	go func() {
		<-time.NewTimer(cb.options.TimeoutState).C

		if cb.GetState() == STATE_OPEN {
			// set new state for service
			cb.SetState(STATE_HALF_OPEN)

			// send event service is unavailable to other proces
			event := &BreakerEvent{Code: SERVICE_AVAILABLE, Message: "Service " + cb.options.NameService + " is available"}
			cb.SendEvent(event)
		}
	}()
}

// execute handle
func (cb *Breaker) Execute(handle func() ResponseCommand, timeout time.Duration) ResponseCommand {

	// add request
	atomic.AddUint32(&cb.counter.TotalRequests, 1)
	response := ResponseCommand{}

	// Reject all invoke when current state is OPEN
	if cb.IsOpen() {
		cb.Reject()
		response.Error = cb.ErrorServiceUnavailable()
		response.Data = nil
		return response
	}

	// run handle immediate and execute time is unlimited
	if timeout == 0 {
		response = handle()
	} else {

		// run handle with time execute is limited
		c := make(chan ResponseCommand, 1)
		go func() {
			c <- handle()
		}()

		select {
		case r := <-c:
			response = r
			close(c)
		case <-time.NewTimer(timeout).C:
			response.Error = cb.ErrorTimeoutExecute()
			response.Data = nil
		}
	}

	// check for any errors
	if response.Error != nil {
		cb.Failure()
	} else {
		cb.Success()
	}
	return response
}

func (s State) toString() string {
	switch s {
	case STATE_CLOSE:
		return "close"
	case STATE_HALF_OPEN:
		return "half_open"
	case STATE_OPEN:
		return "open"
	default:
		return "undefine"
	}
}
