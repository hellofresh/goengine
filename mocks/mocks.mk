MOCKS_BIN=$(CURDIR)/mocks/bin
GOLANG_MOCKS_VERSION=$(shell grep github.com/golang/mock $(CURDIR)/go.mod | awk '{print $$2}')

mocks/mockgen=$(MOCKS_BIN)/mockgen/mockgen
$(mocks/mockgen):
	$(call title, "Installing mockgen")
	mkdir -p "$(@D)"
	GOBIN="$(@D)" go install github.com/golang/mock/mockgen@${GOLANG_MOCKS_VERSION}
