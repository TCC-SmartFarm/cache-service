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

// Estrutura para podermos extrair o ApplicationId e DevAddr do JSON que vem da fila
type IncomingMessage struct {
    UserId string      `json:"userId"`
	ApplicationId   string      `json:"applicationId"`
	DevAddr string      `json:"devAddr"`
    DevEUI     string      `json:"devEUI"`
	Payload  interface{} `json:"payload"`
}

var ctx = context.Background()

func main() {
	// Configurações via Variáveis de Ambiente
	rabbitURL := os.Getenv("RABBIT_URL")
	redisAddr := os.Getenv("REDIS_ADDR")

	log.Printf("Tentando conectar no Redis em: %s", redisAddr)
    log.Printf("Tentando conectar no RabbitMQ em: %s", rabbitURL)

    // Conectar ao Redis
    rdb := redis.NewClient(&redis.Options{
        Addr: redisAddr,
    })
    
    // Testar conexão com Redis (contexto adicionado para o Ping)
    if err := rdb.Ping(ctx).Err(); err != nil {
        log.Fatalf("Erro ao conectar no Redis (%s): %v", redisAddr, err)
    }
    log.Println("Conectado ao Redis")


	// Conectar ao RabbitMQ
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
        log.Printf("Aguardando RabbitMQ iniciar.%v", err)
        time.Sleep(5 * time.Second) // Espera 5 segundos para a próxima tentativa
    }

    if err != nil {
        log.Fatalf("Falha definitiva ao conectar no RabbitMQ: %v", err)
    }

    ch, err := conn.Channel()
    if err != nil {
        log.Fatalf("Falha ao abrir canal no RabbitMQ: %v", err)
    }


	// Garante que a Exchange existe
	err = ch.ExchangeDeclare("telemetria_exchange", "topic", true, false, false, false, nil)

	// Cria uma fila temporária EXCLUSIVA para o Cache-Service
	// O nome vazio "" faz o RabbitMQ gerar um nome tipo amq.gen-XXXX
	q, err := ch.QueueDeclare("", false, false, true, false, nil)

	// O BINDING: Vincula a sua fila temporária à Exchange de tópicos
	// Usando "device.#", qualquer mensagem de qualquer device cairá aqui
	err = ch.QueueBind(q.Name, "device.#", "telemetria_exchange", false, nil)

	// CONSUMO: Agora consumimos da fila que acabamos de vincular
	msgs, err := ch.Consume(q.Name, "cache-service", true, false, false, false, nil)

	log.Printf("[*] Cache-Service conectado à exchange. Ouvindo: %s", q.Name)

	for d := range msgs {
        var msg IncomingMessage
        if err := json.Unmarshal(d.Body, &msg); err != nil {
            continue
        }

        // a chave (removi o ":latest" pois agora é uma lista/histórico curto)
        cacheKey := fmt.Sprintf("userId:%s:devEUI:%s:history", msg.UserId, msg.DevEUI)

        // Pipeline para garantir atomicidade (executa os dois comandos juntos)
        pipe := rdb.Pipeline()

        // Adiciona a nova mensagem no início da lista (Left Push)
        pipe.LPush(ctx, cacheKey, d.Body)

        // Corta a lista para manter apenas os índices de 0 a 19 (total 20 itens)
        pipe.LTrim(ctx, cacheKey, 0, 19)

        // Define ou renova o TTL para 720 horas (30 dias) a cada nova leitura
        pipe.Expire(ctx, cacheKey, 720*time.Hour)

        _, err := pipe.Exec(ctx)
        if err != nil {
            log.Printf("Erro ao atualizar cache circular: %v", err)
        } else {
            log.Printf("Cache circular atualizado: %s (Buffer: 20)", cacheKey)
        }
    }
}