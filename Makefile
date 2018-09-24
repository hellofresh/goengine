### --------------------------------------------------------------------------------------------------------------------
### Variables
### (https://www.gnu.org/software/make/manual/html_node/Using-Variables.html#Using-Variables)
### --------------------------------------------------------------------------------------------------------------------
BUILD_DIR ?= $(CURDIR)/out

POSTGRES_DSN ?= "postgres://goengine:goengine@localhost:8043/goengine?sslmode=disable&client_encoding=UTF8"

### --------------------------------------------------------------------------------------------------------------------
### RULES
### (https://www.gnu.org/software/make/manual/html_node/Rule-Introduction.html#Rule-Introduction)
### --------------------------------------------------------------------------------------------------------------------
.PHONY: all

all: clean deps test

#-----------------------------------------------------------------------------------------------------------------------
# Housekeeping - Cleans our project: deletes binaries
#-----------------------------------------------------------------------------------------------------------------------
.PHONY: clean

clean:
	echo "Cleaning"
	go clean -v

#-----------------------------------------------------------------------------------------------------------------------
# Dependencies
#-----------------------------------------------------------------------------------------------------------------------
.PHONY: deps

deps:
	echo "Installing dependencies"
	dep ensure -v

#-----------------------------------------------------------------------------------------------------------------------
# Testing
#-----------------------------------------------------------------------------------------------------------------------
.PHONY: test test-unit

test: test-unit test-examples

test-unit:
	echo "Running unit tests"
	go test -tags=unit -race ./...

test-integration:
	echo "Running integration tests on ci"
	POSTGRES_DSN=$(POSTGRES_DSN) go test -tags=integration -race ./...

test-examples:
	echo "Running examples"
	go run -race example/aggregate/*.go
	go run -race example/repository/*.go

