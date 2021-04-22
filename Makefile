NO_COLOR=\033[0m
OK_COLOR=\033[32;01m

test-integration:
	@echo "$(OK_COLOR)==> Running integration tests$(NO_COLOR)"
	@STORAGE_DSN=mongodb://localhost:27017/ BROKER_DSN=amqp://guest:guest@localhost:5672/ go run ./cmd/goengine/...

test-unit:
	@echo "$(OK_COLOR)==> Running unit tests$(NO_COLOR)"
	@go test -cover -coverprofile=coverage.txt -covermode=atomic ./...

lint:
	@echo "$(OK_COLOR)==> Linting with golangci-lint$(NO_COLOR)"
	@docker run -it --rm -v $(pwd):/app -w /app golangci/golangci-lint:v1.39.0 golangci-lint run -v
