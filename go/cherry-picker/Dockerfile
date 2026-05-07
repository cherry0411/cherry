FROM golang:alpine AS build
WORKDIR /src
ENV GOPROXY=https://goproxy.cn,direct
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV GOTOOLCHAIN=auto
RUN CGO_ENABLED=0 go build -o /cherry-picker ./cmd/cherry-picker

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /cherry-picker /usr/local/bin/cherry-picker
ENTRYPOINT ["cherry-picker"]
