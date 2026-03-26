FROM golang:1.23 AS builder

WORKDIR /app
COPY go.mod ./
RUN go mod download

COPY main.go .
RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o service-go .

FROM scratch
COPY --from=builder /app/service-go /service-go
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
EXPOSE 8080
ENTRYPOINT ["/service-go"]
