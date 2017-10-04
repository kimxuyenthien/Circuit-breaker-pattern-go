package main

import (
	"circuit-breaker-go/breaker"
	"errors"
	"fmt"
	"gopkg.in/jmcvetta/napping.v3"
	"time"
)

type State string

const (
	Apple = iota
	Banana
	Cherimoya
	Durian
	Elderberry
	Fig
)

type ResponseUserAgent struct {
	Useragent string `json:"user-agent"`
}

func remoteCall() breaker.ResponseCommand {
	// Expensive remote call

	response := &breaker.ResponseCommand{}

	s := napping.Session{}
	url := "http://httpbin.org/user-agent"

	res := ResponseUserAgent{}
	resp, err := s.Get(url, nil, &res, nil)
	if err != nil {
		response.Error = err
	}
	if resp.Status() == 200 {
		response.Data = res.Useragent
	} else {
		response.Error = errors.New("Bad response status from httpbin server")
	}
	return *response
}

func main() {

	circuitBreaker := breaker.NewBreaker(&breaker.OptionsConfig{})
	testChannel := circuitBreaker.Subcriber()
	go func() {

		for event := range testChannel {
			fmt.Println("Receive event ", event)
		}
	}()

	for i := 0; i < 10; i++ {

		response := circuitBreaker.Execute(remoteCall, 100*time.Millisecond)
		fmt.Println(response)
		time.Sleep(800 * time.Millisecond)
	}
}
