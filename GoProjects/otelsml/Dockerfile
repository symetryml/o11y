FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY demclient /src/demclient
COPY godf /src/godf
COPY oteletl /src/oteletl
COPY otelsml /src/otelsml
WORKDIR /src/otelsml
RUN go mod tidy && go build -o /otelsml ./cmd/otelsml/

FROM alpine:3.21
COPY --from=builder /otelsml /otelsml
ENTRYPOINT ["/otelsml"]
