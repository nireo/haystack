.PHONY: tidy
tidy:
	go mod tidy -v
	go fmt ./...


.PHONY: test
test:
	go test -v -race ./...
