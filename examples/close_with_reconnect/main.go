package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/apsyadira-jubelio/go-amqp-reconnect/rabbitmq"
	"github.com/gammazero/workerpool"
	"github.com/streadway/amqp"
)

func main() {
	rabbitmq.Debug = true

	rmqCliet := rabbitmq.NewRmqClient(&rabbitmq.ChannelOptions{
		ConnectionString: "amqp://guest:guest@localhost:5672/",
		QueueName:        "test-auto-delete",
	})

	queueName := "test-auto-delete"
	queueTTL := 1500000
	queueDurable := true
	queueExchange := fmt.Sprintf("%s-exchange", queueName)
	deadQueue := fmt.Sprintf("%s-dead-queue", queueName)
	DeadExchange := fmt.Sprintf("%s-dead-exchange", queueName)
	deadQueueDurable := false
	deadQueueTTL := 1500000

	err := rmqCliet.OpenChannel(&rabbitmq.ChannelOptions{
		ConnectionString: "amqp://guest:guest@localhost:5672/",
		QueueName:        "test-auto-delete",
		QueueTTL:         queueTTL,
		QueueDurable:     queueDurable,
		QueueExchange:    queueExchange,
		DeadQueue:        deadQueue,
		DeadExchange:     DeadExchange,
		DeadQueueDurable: deadQueueDurable,
		DeadQueueTTL:     deadQueueTTL,
		QueueRoutingKey:  "test-auto-delete",
	})

	if err != nil {
		log.Println(err)
	}

	go func() {
		for {
			err := rmqCliet.SendMessage(map[string]interface{}{
				"message": "hello world",
			},
				queueName,
				amqp.Publishing{
					ContentType: "text/plain",
				})

			if err != nil {
				log.Println(err)
			}

			log.Println("publish message")
			time.Sleep(time.Second * 2)
		}
	}()

	// Create a context that is cancelled on system interrupt or SIGTERM signal
	ctx, closeCtx := context.WithCancel(context.Background())
	defer closeCtx()

	consumerDone := make(chan bool, 1)

	// Create a worker pool with a specified number of workers
	wp := workerpool.New(1)
	defer wp.Stop() // Ensure all workers are stopped before exiting

	go func() {
		d, err := rmqCliet.Consume()
		if err != nil {
			log.Panic(err)
		}

		for {
			stop := false

			select {
			case <-ctx.Done():
				stop = true
			case msg := <-d:
				wp.Submit(func() {
					log.Printf("msg: %s", string(msg.Body))
					// Acknowledge the message
					msg.Ack(false)
				})
			}

			if stop {
				break
			}
		}

		consumerDone <- true
	}()

	signals := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		s := <-signals
		log.Printf("received signal: %v", s)
		closeCtx()

		<-consumerDone
		rmqCliet.Close()

		done <- true
	}()

	log.Printf("awaiting signal")
	<-done
	wp.StopWait()
	log.Printf("exiting")
}
