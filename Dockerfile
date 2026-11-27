# 构建阶段：静态编译
FROM golang:1.27 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/gateway ./cmd/gateway

# 运行阶段：最小镜像
FROM alpine:3.21
RUN adduser -D -u 10001 gateway && apk add --no-cache ca-certificates tzdata
USER gateway
WORKDIR /app
COPY --from=builder /out/gateway /app/gateway
COPY configs/config.yaml /app/configs/config.yaml
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://127.0.0.1:8080/healthz || exit 1
ENTRYPOINT ["/app/gateway"]
CMD ["-conf", "/app/configs/config.yaml"]
