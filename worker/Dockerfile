# build
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN go mod tidy && go build -o /worker .

# runtime enxuto
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /worker /worker
ENTRYPOINT ["/worker"]
