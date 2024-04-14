package rabbitmq

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// QueueExchangeType represents types of RabbitMQ exchanges
type QueueExchangeType string

const (
	Direct        QueueExchangeType = "direct"
	Fanout        QueueExchangeType = "fanout"
	Delayed       QueueExchangeType = "x-delayed-message"
	Deduplication QueueExchangeType = "x-message-deduplication"
)

// ChannelOptions holds all configuration options for the channel
type ChannelOptions struct {
	ConnectionString     string
	QueueName            string
	QueueTTL             int  // Pointer to int, because it's optional
	QueueDurable         bool // Pointer to bool, to handle the optional case
	QueueExchangeType    QueueExchangeType
	QueueExchangeOptions amqp.Table
	QueueExchange        string
	DeadQueue            string
	DeadExchange         string
	DeadQueueDurable     bool
	DeadQueueTTL         int
	Arguments            amqp.Table
	QueueRoutingKey      string
}

// RmqClient represents the RabbitMQ Client
type RmqClient struct {
	Options *ChannelOptions
	Channel *Channel
	Conn    *Connection
}

// NewRmqClient creates a new instance of RmqClient
func NewRmqClient(options *ChannelOptions) *RmqClient {
	conn, err := Dial(options.ConnectionString)
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %v", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel: %v", err)
	}

	return &RmqClient{
		Options: options,
		Channel: channel,
		Conn:    conn,
	}
}

// OpenChannel opens and configures a channel with the specified options
func (client *RmqClient) OpenChannel(options *ChannelOptions) error {
	if client.Conn.IsClosed() {
		return fmt.Errorf("connection is closed")
	}

	if options.QueueName == "" {
		return fmt.Errorf("queueName is required")
	}

	// create exchange if QueueExchange is defined
	if options.QueueExchange != "" {
		exchangeType := "direct" // default exchange type
		if options.QueueExchangeType != "" {
			exchangeType = string(options.QueueExchangeType)
		}

		// create exchange
		err := client.Channel.ExchangeDeclare(
			options.QueueExchange,
			exchangeType,
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	queueOptions := amqp.Table{}
	if options.QueueTTL != 0 {
		queueOptions["x-message-ttl"] = options.QueueTTL
	}
	if options.Arguments != nil {
		for key, value := range options.Arguments {
			queueOptions[key] = value
		}
	}

	// create queue if QueueName is defined
	_, err := client.Channel.QueueDeclare(
		options.QueueName,
		true,
		false,
		false,
		false,
		queueOptions,
	)

	if err != nil {
		return err
	}

	// bind queue to exchange if QueueExchange is defined
	if options.QueueExchange != "" && options.QueueRoutingKey != "" {
		err = client.Channel.QueueBind(
			options.QueueName,
			"",
			options.QueueExchange,
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	// create dead letter queue if DeadQueue is defined and DeadExchange is defined
	if options.DeadExchange != "" && options.DeadQueue != "" {
		deadQueueOptions := amqp.Table{}
		if options.DeadQueueTTL != 0 {
			deadQueueOptions["x-message-ttl"] = options.DeadQueueTTL
		}

		err = client.Channel.ExchangeDeclare(
			options.DeadExchange,
			"fanout",
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return err
		}

		_, err = client.Channel.QueueDeclare(
			options.DeadQueue,
			true,
			false,
			false,
			false,
			deadQueueOptions,
		)
		if err != nil {
			return err
		}

		err = client.Channel.QueueBind(
			options.DeadQueue,
			"",
			options.DeadExchange,
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	client.Channel.Qos(1, 0, false)
	client.Options = options

	return nil
}

func (client *RmqClient) Consume() (<-chan amqp.Delivery, error) {
	if client.Channel == nil {
		return nil, fmt.Errorf("please open an RMQ channel first")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		client.waitForReconnect()
		defer wg.Done()
	}()
	wg.Wait()

	msgs, err := client.Channel.Consume(
		client.Options.QueueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return msgs, nil

}

// getMessage receives a single message from the specified queue, acknowledging it if found
func (client *RmqClient) GetMessage() (*amqp.Delivery, error) {
	if client.Channel == nil {
		return nil, fmt.Errorf("please open an RMQ channel first")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		client.waitForReconnect()
		defer wg.Done()
	}()
	wg.Wait()

	msg, ok, err := client.Channel.Get(client.Options.QueueName, false) // false means no auto-ack
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	// Acknowledge the message
	err = client.Channel.Ack(msg.DeliveryTag, false)
	if err != nil {
		return nil, fmt.Errorf("failed to ack message: %v", err)
	}

	return &msg, nil
}

// sendMessage sends a message to the specified exchange
func (client *RmqClient) SendMessage(msg interface{}, routingKey string, publishOptions amqp.Publishing) error {
	if client.Channel == nil || client.Options.QueueExchange == "" {
		return fmt.Errorf("please open an RMQ channel first and define queueExchange options")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		client.waitForReconnect()
		defer wg.Done()
	}()
	wg.Wait()

	if publishOptions.ContentType == "" {
		publishOptions.ContentType = "text/plain"
	}

	messageBody, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	publishOptions.Body = messageBody
	if err := client.Channel.Publish(
		"",         // Exchange
		routingKey, // Routing key
		false,      // Mandatory
		false,      // Immediate
		publishOptions,
	); err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}

	return nil
}

// close cleanly shuts down the channel and connection
func (client *RmqClient) Close() error {
	if client.Channel != nil {
		if err := client.Channel.Close(); err != nil {
			return fmt.Errorf("failed to close channel: %v", err)
		}
	}
	if client.Conn != nil {
		if err := client.Conn.Close(); err != nil {
			return fmt.Errorf("failed to close connection: %v", err)
		}
	}
	client.Channel = nil
	client.Conn = nil
	client.Options = nil
	return nil
}

func (client *RmqClient) waitForReconnect() {
	if client.Conn.IsClosed() {
		log.Println(" reconnecting...")

		time.Sleep(3 * time.Second)

	}
}
