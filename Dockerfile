# Estágio 1: Build
FROM golang:1.24-alpine AS builder

# Instalar dependências necessárias para o build
RUN apk add --no-cache git

# Definir diretório de trabalho
WORKDIR /app

# Copiar os arquivos de módulos e baixar dependências
COPY go.mod go.sum ./
RUN go mod download

# Copiar o código fonte
COPY . .

# Compilar o binário de forma estática (remove dependências do C e reduz o tamanho)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o cache-service .

# Estágio 2: Execução (Imagem Final)
FROM alpine:latest

# Instalar certificados CA (necessário se o seu serviço for falar com APIs HTTPS no futuro)
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copiar apenas o binário do estágio de build
COPY --from=builder /app/cache-service .

# Comando para rodar o serviço
CMD ["./cache-service"]