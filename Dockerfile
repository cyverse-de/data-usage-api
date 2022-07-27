FROM golang:1.18 as build-root

WORKDIR /go/src/github.com/cyverse-de/data-usage-api

COPY go.mod .
COPY go.sum .

COPY . .

ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64

RUN go build --buildvcs=false .
RUN go clean -cache -modcache
RUN cp ./data-usage-api /bin/data-usage-api

ENTRYPOINT ["data-usage-api"]

EXPOSE 60000
