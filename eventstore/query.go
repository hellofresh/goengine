package eventstore

import (
	"context"

	goengine_dev "github.com/hellofresh/goengine-dev"
)

type (
	// QueryMessageHandler is a func that can do state changes based on a message
	QueryMessageHandler func(ctx context.Context, state interface{}, message goengine_dev.Message) (interface{}, error)

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
	//  		"deposited": func(ctx context.Context, state interface{}, message goengine_dev.Message) (interface{}, error) {
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
)
