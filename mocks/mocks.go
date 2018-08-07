package mocks

import "github.com/stretchr/testify/mock"

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
