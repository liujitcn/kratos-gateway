# 构建阶段：静态编译
FROM golang:1.27 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/gateway ./internal/cmd/server

# 运行阶段：最小镜像
FROM alpine:3.21
RUN adduser -D -u 10001 gateway && apk add --no-cache ca-certificates tzdata \
    && mkdir -p /app/configs && chown gateway:gateway /app/configs
USER gateway
WORKDIR /app
COPY --from=builder /out/gateway /app/gateway
COPY --chown=gateway:gateway configs/*.yaml /app/configs/
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=3s CMD wget -qO- http://127.0.0.1:8080/readyz || exit 1
ENTRYPOINT ["/app/gateway"]
CMD ["-c", "/app/configs", "-e", "prod"]
