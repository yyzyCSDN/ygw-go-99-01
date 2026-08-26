FROM golang:1.23.12 AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY vendor ./vendor
COPY cmd ./cmd
COPY internal ./internal
COPY web ./web
RUN go build -mod=vendor -o /out/coalminegas ./cmd/server

FROM golang:1.23.12
WORKDIR /app
COPY go.mod go.sum ./
COPY vendor ./vendor
COPY cmd ./cmd
COPY internal ./internal
COPY web ./web
COPY --from=build /out/coalminegas /app/coalminegas
ENV GOPROXY=off GOSUMDB=off
EXPOSE 8080
CMD ["/app/coalminegas"]
