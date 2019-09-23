# Aggregate Projector using a Broker

This is an example showing how to use GoEngine together with a RabbitMQ aggregate projector that uses RabbitMQ.

export POSTGRES_DSN="postgres://goengine:goengine@localhost/goengine-bank?sslmode=disable&client_encoding=UTF8"
export AMQP_DSN="amqp://goengine:goengine@localhost:5672/"

docker-compose -f example/broker/docker-compose.yml up -d
go run example/broker/sql/*.go
go run example/broker/projector/*.go
go run example/broker/dispatcher/main.go
go run example/broker/app/main.go 
