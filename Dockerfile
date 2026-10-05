FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /clab-grpc .

FROM alpine:3
RUN apk add --no-cache ca-certificates openssh-client
COPY --from=build /clab-grpc /usr/local/bin/clab-grpc
ENTRYPOINT ["clab-grpc"]
CMD ["-port", "8091"]
