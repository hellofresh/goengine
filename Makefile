NO_COLOR=\033[0m
OK_COLOR=\033[32;01m

format:
	@echo "$(OK_COLOR)==> checking code formating with 'gofmt' tool$(NO_COLOR)"
	@gofmt -l -s cmd pkg | grep ".*\.go"; if [ "$$?" = "0" ]; then exit 1; fi

vet:
	@echo "$(OK_COLOR)==> checking code correctness with 'go vet' tool$(NO_COLOR)"
	@go vet ./...

lint: tools.golint
	@echo "$(OK_COLOR)==> checking code style with 'golint' tool$(NO_COLOR)"
	@go list ./... | xargs -n 1 golint -set_exit_status

test-integration: lint format vet
	@echo "$(OK_COLOR)==> Running integration tests$(NO_COLOR)"
	@STORAGE_DSN=mongodb://localhost:27017/ BROKER_DSN=amqp://guest:guest@localhost:5672/ go run ./cmd/goengine/...

test-unit: lint format vet
	@echo "$(OK_COLOR)==> Running unit tests$(NO_COLOR)"
	@ginkgo -r

#---------------
#-- tools
#---------------

tools: tools.golint

tools.golint:
	@command -v golint >/dev/null ; if [ $$? -ne 0 ]; then \
		echo "--> installing golint"; \
		go get github.com/golang/lint/golint; \
	fi
