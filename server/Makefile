.PHONY: rm build

VERSION=$(shell git describe --always --long --dirty)

version:
	 @echo Version IS $(VERSION)

run:
	./server

rm:
	rm -v bin/reporter-linux-amd64; rm ~/linkit/reporter-linux-amd64;

build:
	export GOOS=linux; export GOARCH=amd64; \
        go build -ldflags "-s -w" -o bin/reporter-linux-amd64;

cp:
	cp -v bin/reporter-linux-amd64 ~/linkit; cp -v dev/reporter.yml ~/linkit/;

tail:
	tail -10 /var/log/linkit/reporter.log

call:
	curl -H 'Content-Type: application/json' -H 'Accept: application/json' --data-binary '{"jsonrpc":"1.0","id":"curltext","method":"SVC.ByHash","params":{ "hash": "f90f2aca5c640289d0a29417bcb63a37"}}' -H 'content-type:text/plain;' http://localhost:50307/

buildalpine:
	CC=$(which musl-gcc) go build --ldflags '-w -linkmode external -extldflags "-static"' -o  bin/reporter-alpine