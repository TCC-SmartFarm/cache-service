package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

// Estrutura para extrair os identificadores e o timestamp do JSON que vem da fila
type IncomingMessage struct {
	UserId        string `json:"userId"`
	ApplicationId string `json:"applicationId"`
	DevAddr       string `json:"devAddr"`
	DevEUI        string `json:"devEUI"`
	Payload       struct {
		Timestamp int64 `json:"timestamp"`
	} `json:"payload"`
}

const (
	queueName    = "fila_cache"
	exchangeName = "telemetria_exchange"
	routingKey   = "device.#"
	cacheSize    = 20
	cacheTTL     = 720 * time.Hour // 30 dias, igual à janela -30d das consultas ao Influx
)

// MESMA chave usada pelo api-service. É um SORTED SET (score = timestamp), e não
// mais uma lista: assim o Redis fica ordenado pelo horário da leitura e sem
// duplicatas, exatamente como o Influx (1 ponto por timestamp).
func cacheKey(userId, devEUI string) string {
	return fmt.Sprintf("userId:%s:devEUI:%s:readings", userId, devEUI)
}

var ctx = context.Background()

func main() {
	rabbitURL := os.Getenv("RABBIT_URL")
	if rabbitURL == "" {
		rabbitURL = "amqp://admin:admin@barramento-de-eventos:5672/"
	}
	redisAddr := os.Getenv("REDIS_ADDR")

	log.Printf("Tentando conectar no Redis em: %s", redisAddr)

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Erro ao conectar no Redis (%s): %v", redisAddr, err)
	}
	log.Println("Conectado ao Redis")

	// Conectar ao RabbitMQ (antes a URL era fixa no código e ignorava RABBIT_URL)
	var conn *amqp.Connection
	var err error
	for i := 1; i <= 10; i++ {
		fmt.Printf("Tentando conectar ao RabbitMQ (Tentativa %d/10)...\n", i)
		conn, err = amqp.Dial(rabbitURL)
		if err == nil {
			break
		}
		log.Printf("Aguardando RabbitMQ iniciar. %v", err)
		time.Sleep(5 * time.Second)
	}
	if err != nil {
		log.Fatalf("Falha definitiva ao conectar no RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Falha ao abrir canal no RabbitMQ: %v", err)
	}

	if err := ch.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil); err != nil {
		log.Fatalf("Erro ao declarar exchange: %v", err)
	}
	// Fila DURÁVEL e nomeada (antes era temporária/exclusiva: tudo o que chegava
	// enquanto o cache-service estava fora era perdido).
	if _, err := ch.QueueDeclare(queueName, true, false, false, false, nil); err != nil {
		log.Fatalf("Erro ao declarar fila %s: %v", queueName, err)
	}
	if err := ch.QueueBind(queueName, routingKey, exchangeName, false, nil); err != nil {
		log.Fatalf("Erro no bind da fila %s: %v", queueName, err)
	}
	if err := ch.Qos(10, 0, false); err != nil {
		log.Fatalf("Erro ao configurar prefetch: %v", err)
	}

	msgs, err := ch.Consume(queueName, "cache-service", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("Erro ao consumir %s: %v", queueName, err)
	}

	log.Printf("[*] Cache-Service conectado à exchange. Ouvindo: %s", queueName)

	for d := range msgs {
		var msg IncomingMessage
		if err := json.Unmarshal(d.Body, &msg); err != nil {
			log.Printf("JSON inválido (descartado): %v", err)
			_ = d.Nack(false, false)
			continue
		}

		userId := strings.TrimSpace(msg.UserId)
		devEUI := strings.ToLower(strings.TrimSpace(msg.DevEUI))
		if userId == "" || devEUI == "" {
			log.Printf("Mensagem sem userId/devEUI (descartada): %s", string(d.Body))
			_ = d.Nack(false, false)
			continue
		}

		ts := msg.Payload.Timestamp
		if ts <= 0 {
			ts = time.Now().Unix()
		}
		key := cacheKey(userId, devEUI)
		score := strconv.FormatInt(ts, 10)

		// Transação atômica:
		//  1. remove leitura anterior com o MESMO timestamp (reentrega do MQTT/RabbitMQ)
		//  2. insere a leitura com score = timestamp
		//  3. mantém só as 20 mais recentes
		//  4. renova o TTL
		pipe := rdb.TxPipeline()
		pipe.ZRemRangeByScore(ctx, key, score, score)
		pipe.ZAdd(ctx, key, redis.Z{Score: float64(ts), Member: string(d.Body)})
		pipe.ZRemRangeByRank(ctx, key, 0, -(cacheSize + 1))
		pipe.Expire(ctx, key, cacheTTL)

		if _, err := pipe.Exec(ctx); err != nil {
			log.Printf("Erro ao atualizar cache (vai tentar de novo): %v", err)
			time.Sleep(time.Second)
			_ = d.Nack(false, true) // devolve para a fila; Redis volta e a leitura não se perde
			continue
		}
		_ = d.Ack(false)
		log.Printf("Cache atualizado: %s (ts=%d, buffer: %d)", key, ts, cacheSize)
	}

	log.Fatal("Canal do RabbitMQ fechado; encerrando para reinício")
}
