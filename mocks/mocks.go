package mocks

import "github.com/stretchr/testify/mock"

// In order to make sure that we have the same mocks we can regenerate them using `go generate`
//go:generate go run ../vendor/github.com/vektra/mockery/cmd/mockery/mockery.go -dir ../ -name=EventStore -outpkg mocks -output ./ -case underscore
//go:generate go run ../vendor/github.com/vektra/mockery/cmd/mockery/mockery.go -dir ../ -name=EventStream -outpkg mocks -output ./ -case underscore
//go:generate go run ../vendor/github.com/vektra/mockery/cmd/mockery/mockery.go -dir ../ -name=MessagePayloadFactory -outpkg mocks -output ./ -case underscore
//go:generate go run ../vendor/github.com/vektra/mockery/cmd/mockery/mockery.go -dir ../ -name=MessagePayloadConverter -outpkg mocks -output ./ -case underscore
//go:generate go run ../vendor/github.com/vektra/mockery/cmd/mockery/mockery.go -dir ../ -name=MessagePayloadResolver -outpkg mocks -output ./ -case underscore
//go:generate go run ../vendor/github.com/vektra/mockery/cmd/mockery/mockery.go -dir ../driver/sql/ -name=PersistenceStrategy -outpkg sql -output ./driver/sql -case underscore
//go:generate go run ../vendor/github.com/vektra/mockery/cmd/mockery/mockery.go -dir ../ -name=Query -outpkg mocks -output ./ -case underscore
//go:generate go run ../vendor/github.com/vektra/mockery/cmd/mockery/mockery.go -dir ../driver/sql/ -name=MessageFactory -outpkg sql -output ./driver/sql -case underscore
//go:generate go run ../vendor/github.com/vektra/mockery/cmd/mockery/mockery.go -dir ../ -name=Message -outpkg mocks -output ./ -case underscore

// FetchFuncCalls a helper go fetch all calls made to a func
func FetchFuncCalls(calls []mock.Call, funcName string) []mock.Call {
	funcCalls := make([]mock.Call, 0, len(calls))
	for _, call := range calls {
		if call.Method != funcName {
			continue
		}

		funcCalls = append(funcCalls, call)
	}

	return funcCalls
}
