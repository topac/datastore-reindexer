# Stage 1
FROM golang:1.14-alpine as builder
RUN apk add git unzip gcc g++ make ca-certificates

RUN mkdir /workspace

COPY . /workspace/
WORKDIR /workspace

RUN go test ./...
RUN go build -o /datastorefix -ldflags="-s -w" .

# Stage 2
FROM alpine as prod

RUN apk update && \
    apk --no-cache add ca-certificates && \
    rm -rf /var/cache/apk/*

COPY --from=builder /datastorefix /
ENTRYPOINT ["/datastorefix"]
