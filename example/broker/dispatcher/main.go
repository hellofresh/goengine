package main

import (
	"context"
	"github.com/hellofresh/goengine/example/broker/lib"
	"github.com/hellofresh/goengine/extension/amqp"
	goengineZap "github.com/hellofresh/goengine/extension/zap"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewDevelopment()
	failOnErr(err)

	goengineLogger := goengineZap.Wrap(logger)

	listener, err := lib.NewGoEngineListener(goengineLogger)
	failOnErr(err)

	publisher, err := amqp.NewNotificationPublisher(
		lib.AMQPDSN,
		"goengine_example",
		goengineLogger,
	)
	failOnErr(err)

	ctx := context.Background();

	// TODO os.signal
	failOnErr(listener.Listen(ctx, publisher.Publish))
}

func failOnErr(err error) {
	if err != nil {
		panic(err)
	}
}
