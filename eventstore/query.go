package eventstore

import (
	"context"

	"github.com/hellofresh/goengine/messaging"
)

type (
	// QueryMessageHandler is a func that can do state changes based on a message
	QueryMessageHandler func(state interface{}, message messaging.Message) (interface{}, error)

	// Query contains the information of a query
	//
	// Example when querying the total the amount of deposits the query could be as follows.
	//  type TotalDepositState struct {
	//  	deposited int
	//  	times     int
	//  }
	//
	//  type TotalDepositQuery struct {}
	//  func (q *TotalDepositQuery) Init() interface{} {
	//  	return TotalDepositState{}
	//  }
	//  func (q *TotalDepositQuery) Handlers() interface{} {
	//  	return map[string]QueryMessageHandler{
	//  		"deposited": func(state interface{}, message messaging.Message) (interface{}, error) {
	//  			depositState := state.(TotalDepositState)
	//
	//  			switch event := message.Payload().(type) {
	//  			case AccountDebited:
	//  				depositState.deposited += event.Amount
	//  			}
	//
	//  			return depositState, nil
	//  		},
	//  	}
	//  }
	Query interface {
		// init initializes the state of the Query.
		Init() interface{}

		// handlers are functions that handle a message and return the new state
		//
		// For
		Handlers() map[string]QueryMessageHandler
	}

	// QueryExecutor is an interface that will allow you to run a query
	QueryExecutor interface {
		// Reset returns the query to it's initial state
		Reset(ctx context.Context)
		// Run executes the query and returns the final state
		Run(ctx context.Context) (interface{}, error)
	}
)
