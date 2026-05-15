package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

// Estrutura para podermos extrair o UserId e DeviceId do JSON que vem da fila
type IncomingMessage struct {
	UserId   string      `json:"userId"`
	DeviceId string      `json:"deviceId"`
	Payload  interface{} `json:"payload"`
}

var ctx = context.Background()

func main() {
	// 1. Configurações via Variáveis de Ambiente
	rabbitURL := os.Getenv("RABBIT_URL")
	redisAddr := os.Getenv("REDIS_ADDR")

	log.Printf("Tentando conectar no Redis em: %s", redisAddr)
    log.Printf("Tentando conectar no RabbitMQ em: %s", rabbitURL)

    // 2. Conectar ao Redis
    rdb := redis.NewClient(&redis.Options{
        Addr: redisAddr,
    })
    
    // Testar conexão com Redis (contexto adicionado para o Ping)
    if err := rdb.Ping(ctx).Err(); err != nil {
        log.Fatalf("Erro ao conectar no Redis (%s): %v", redisAddr, err)
    }
    log.Println("Conectado ao Redis")


	// 3. Conectar ao RabbitMQ
	var conn *amqp.Connection
    var err error
    url := "amqp://admin:admin@barramento-de-eventos:5672/" 

    // Tenta conectar 5 vezes antes de desistir
    for i := 1; i <= 5; i++ {
        fmt.Printf("Tentando conectar ao RabbitMQ (Tentativa %d/5)...\n", i)
        conn, err = amqp.Dial(url)
        if err == nil {
            break
        }
        log.Printf("Aguardando RabbitMQ iniciar... %v", err)
        time.Sleep(5 * time.Second) // Espera 5 segundos para a próxima tentativa
    }

    if err != nil {
        log.Fatalf("Falha definitiva ao conectar no RabbitMQ: %v", err)
    }

    ch, err := conn.Channel()
    if err != nil {
        log.Fatalf("Falha ao abrir canal no RabbitMQ: %v", err)
    }


	// 1. Garante que a Exchange existe
	err = ch.ExchangeDeclare("telemetria_exchange", "topic", true, false, false, false, nil)

	// 2. Cria uma fila temporária EXCLUSIVA para o Cache-Service
	// O nome vazio "" faz o RabbitMQ gerar um nome tipo amq.gen-XXXX
	q, err := ch.QueueDeclare("", false, false, true, false, nil)

	// 3. O BINDING: Vincula a sua fila temporária à Exchange de tópicos
	// Usando "sensor.#", qualquer mensagem de qualquer sensor cairá aqui
	err = ch.QueueBind(q.Name, "sensor.#", "telemetria_exchange", false, nil)

	// 4. CONSUMO: Agora consumimos da fila que acabamos de vincular
	msgs, err := ch.Consume(q.Name, "cache-service", true, false, false, false, nil)

	log.Printf("[*] Cache-Service conectado à exchange. Ouvindo: %s", q.Name)

	for d := range msgs {
		var msg IncomingMessage
		if err := json.Unmarshal(d.Body, &msg); err != nil {
			continue
		}

		// Salva no Redis usando o padrão que combinamos
		cacheKey := fmt.Sprintf("userId:%s:deviceId:%s:latest", msg.UserId, msg.DeviceId)
		rdb.Set(ctx, cacheKey, d.Body, 24*time.Hour)
		
		log.Printf("Cache atualizado via Exchange: %s", cacheKey)
	}
}