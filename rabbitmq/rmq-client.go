package rabbitmq

import (
	"encoding/json"
	"fmt"

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
	QueueTTL             *int  // Pointer to int, because it's optional
	QueueDurable         *bool // Pointer to bool, to handle the optional case
	QueueExchangeType    *QueueExchangeType
	QueueExchangeOptions amqp.Table
	QueueExchange        *string
	DeadQueue            *string
	DeadExchange         *string
	DeadQueueDurable     *bool
	DeadQueueTTL         *int
	Arguments            amqp.Table
	QueueRoutingKey      *string
}

// RmqClient represents the RabbitMQ Client
type RmqClient struct {
	options *ChannelOptions
	channel *Channel
	conn    *Connection
}

// NewRmqClient creates a new instance of RmqClient
func NewRmqClient(options *ChannelOptions) *RmqClient {
	return &RmqClient{
		options: options,
	}
}

// OpenChannel opens and configures a channel with the specified options
func (client *RmqClient) OpenChannel(options *ChannelOptions) error {
	conn, err := Dial(options.ConnectionString)
	if err != nil {
		return err
	}
	client.conn = conn

	client.channel, err = client.conn.Channel()
	if err != nil {
		return err
	}

	if options.QueueExchange != nil {
		exchangeType := "direct" // default exchange type
		if options.QueueExchangeType != nil {
			exchangeType = string(*options.QueueExchangeType)
		}
		err = client.channel.ExchangeDeclare(
			*options.QueueExchange,
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
	if options.QueueTTL != nil {
		queueOptions["x-message-ttl"] = *options.QueueTTL
	}
	if options.Arguments != nil {
		for key, value := range options.Arguments {
			queueOptions[key] = value
		}
	}

	_, err = client.channel.QueueDeclare(
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

	if options.DeadExchange != nil && options.DeadQueue != nil {
		deadQueueOptions := amqp.Table{}
		if options.DeadQueueTTL != nil {
			deadQueueOptions["x-message-ttl"] = *options.DeadQueueTTL
		}

		err = client.channel.ExchangeDeclare(
			*options.DeadExchange,
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

		_, err = client.channel.QueueDeclare(
			*options.DeadQueue,
			true,
			false,
			false,
			false,
			deadQueueOptions,
		)
		if err != nil {
			return err
		}

		err = client.channel.QueueBind(
			*options.DeadQueue,
			"",
			*options.DeadExchange,
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	if options.QueueExchange != nil && options.QueueRoutingKey != nil {
		err = client.channel.QueueBind(
			options.QueueName,
			*options.QueueRoutingKey,
			*options.QueueExchange,
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	client.channel.Qos(1, 0, false)

	client.options = options

	return nil
}

// getMessage receives a single message from the specified queue, acknowledging it if found
func (client *RmqClient) GetMessage() (*amqp.Delivery, error) {
	if client.channel == nil {
		return nil, fmt.Errorf("please open an RMQ channel first")
	}

	msg, ok, err := client.channel.Get(client.options.QueueName, false) // false means no auto-ack
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	// Acknowledge the message
	err = client.channel.Ack(msg.DeliveryTag, false)
	if err != nil {
		return nil, fmt.Errorf("failed to ack message: %v", err)
	}

	return &msg, nil
}

// sendMessage sends a message to the specified exchange
func (client *RmqClient) SendMessage(msg interface{}, routingKey string, publishOptions amqp.Publishing) error {
	if client.channel == nil || client.options.QueueExchange == nil {
		return fmt.Errorf("please open an RMQ channel first and define queueExchange options")
	}

	messageBytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("error marshaling message: %v", err)
	}

	publishOptions.Body = messageBytes

	if err := client.channel.Publish(
		*client.options.QueueExchange, // Exchange
		routingKey,                    // Routing key
		false,                         // Mandatory
		false,                         // Immediate
		publishOptions,
	); err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}

	return nil
}

// close cleanly shuts down the channel and connection
func (client *RmqClient) Close() error {
	if client.channel != nil {
		if err := client.channel.Close(); err != nil {
			return fmt.Errorf("failed to close channel: %v", err)
		}
	}
	if client.conn != nil {
		if err := client.conn.Close(); err != nil {
			return fmt.Errorf("failed to close connection: %v", err)
		}
	}
	client.channel = nil
	client.conn = nil
	client.options = nil
	return nil
}
