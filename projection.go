package goengine_dev

import "context"

type (
	// MessageHandler is a func that can do state changes based on a message
	MessageHandler func(ctx context.Context, state interface{}, message Message) (interface{}, error)

	// Query contains the information of a query
	//
	// Example when querying the total the amount of deposits the query could be as follows.
	//  type TotalDepositState struct {
	//  	deposited int
	//  	times     int
	//  }
	//
	//  type TotalDepositQuery struct {}
	//  func (q *TotalDepositQuery) Init(ctx context.Context) (interface{}, error) {
	//  	return TotalDepositState{}, nil
	//  }
	//  func (q *TotalDepositQuery) Handlers() interface{} {
	//  	return map[string]MessageHandler{
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
		// init initializes the state of the Query
		Init(ctx context.Context) (interface{}, error)

		// Handlers return the handlers for a set of messages
		Handlers() map[string]MessageHandler
	}

	// Projection contains the information of a projection
	Projection interface {
		Query

		// Name returns the name of the projection
		Name() string

		// FromStream returns the stream this projection is based on
		FromStream() StreamName

		// ReconstituteState reconstitute the projection state based on the provided state data
		ReconstituteState(data []byte) (interface{}, error)
	}
)
