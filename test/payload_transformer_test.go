// +build unit

package test_test

import (
	"testing"

	"github.com/hellofresh/goengine/strategy"
	"github.com/hellofresh/goengine/test/internal"
)

type (
	BenchmarkPayload struct {
		Name    string
		Balance int
	}
)

type BenchmarkSuite struct {
	transformer    func() strategy.PayloadTransformer
	payloadCreator strategy.PayloadInitiator
	payload        interface{}
	wireData       []byte
}

var suites = map[string]BenchmarkSuite{
	"Protobuf with pointer": {
		transformer: func() strategy.PayloadTransformer {
			return strategy.NewProtobufPayloadTransformer()
		},
		payloadCreator: func() interface{} {
			return &internal.Payload{}
		},
		payload: &internal.Payload{
			Name:    "Hitchhiker",
			Balance: 42,
		},
		wireData: []byte{10, 10, 72, 105, 116, 99, 104, 104, 105, 107, 101, 114, 16, 42},
	},
	"Protobuf with non-pointer": {
		transformer: func() strategy.PayloadTransformer {
			return strategy.NewProtobufPayloadTransformer()
		},
		payloadCreator: func() interface{} {
			return internal.Payload{}
		},
		payload: internal.Payload{
			Name:    "Hitchhiker",
			Balance: 42,
		},
		wireData: []byte{10, 10, 72, 105, 116, 99, 104, 104, 105, 107, 101, 114, 16, 42},
	},
	"JSON with pointer": {
		transformer: func() strategy.PayloadTransformer {
			return strategy.NewJSONPayloadTransformer()
		},
		payloadCreator: func() interface{} {
			return &BenchmarkPayload{}
		},
		payload: &BenchmarkPayload{
			Name:    "Hitchhiker",
			Balance: 42,
		},
		wireData: []byte("{\"name\":\"Hitchhiker\",\"Balance\":42}"),
	},
	"JSON with non-pointer": {
		transformer: func() strategy.PayloadTransformer {
			return strategy.NewJSONPayloadTransformer()
		},
		payloadCreator: func() interface{} {
			return BenchmarkPayload{}
		},
		payload: BenchmarkPayload{
			Name:    "Hitchhiker",
			Balance: 42,
		},
		wireData: []byte("{\"name\":\"Hitchhiker\",\"Balance\":42}"),
	},
}

func BenchmarkPayloadTransformerConvertPayload(b *testing.B) {
	for name, suite := range suites {
		b.Run(name, func(b *testing.B) {
			transformer := suite.transformer()
			err := transformer.RegisterPayload("benchmark_payload", suite.payloadCreator)
			if err != nil {
				b.Error(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _, err := transformer.ConvertPayload(suite.payload)
				if err != nil {
					b.Error(err)
				}
			}
		})
	}
}

func BenchmarkPayloadTransformerCreatePayload(b *testing.B) {
	for name, suite := range suites {
		b.Run(name, func(b *testing.B) {
			transformer := suite.transformer()
			err := transformer.RegisterPayload("benchmark_payload", suite.payloadCreator)
			if err != nil {
				b.Error(err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := transformer.CreatePayload("benchmark_payload", suite.wireData)
				if err != nil {
					b.Error(err)
				}
			}
		})
	}
}
