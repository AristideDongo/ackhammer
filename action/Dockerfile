FROM golang:1.23
WORKDIR /app
COPY . .
RUN go build ./...
ENTRYPOINT ["go", "test", "./..."]
