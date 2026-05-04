FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /cherry-picker ./cmd/cherry-picker

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /cherry-picker /usr/local/bin/cherry-picker
ENTRYPOINT ["cherry-picker"]
