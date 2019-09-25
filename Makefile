NO_COLOR=\033[0m
OK_COLOR=\033[32;01m

PKG_SRC := github.com/hellofresh/goengine

deps:
	@echo "$(OK_COLOR)==> Installing dependencies$(NO_COLOR)"
	@go get -u github.com/onsi/ginkgo/ginkgo
	@go get -u github.com/onsi/gomega

vet:
	@echo "$(OK_COLOR)==> checking code correctness with 'go vet' tool$(NO_COLOR)"
	@go vet ./...

lint: tools.golint
	@echo "$(OK_COLOR)==> checking code style with 'golint' tool$(NO_COLOR)"
	@go list ./... | xargs -n 1 golint -set_exit_status

test-integration: lint vet
	@echo "$(OK_COLOR)==> Running integration tests$(NO_COLOR)"
	@STORAGE_DSN=mongodb://localhost:27017/ BROKER_DSN=amqp://guest:guest@localhost:5672/ go run $(PKG_SRC)/cmd/goengine/...

test-unit: lint vet
	@echo "$(OK_COLOR)==> Running unit tests$(NO_COLOR)"
	@ginkgo -r

#---------------
#-- tools
#---------------

tools: tools.golint

tools.golint:
	@command -v golint >/dev/null ; if [ $$? -ne 0 ]; then \
		echo "--> installing golint"; \
		go get golang.org/x/lint/golint; \
	fi
