module rabbitmq

go 1.20

require (
	github.com/apsyadira-jubelio/go-amqp-reconnect/rabbitmq v0.0.0-20240414094204-454afb52b150
	github.com/gammazero/workerpool v1.1.3
	github.com/streadway/amqp v1.1.0
)

require github.com/gammazero/deque v0.2.0 // indirect

replace github.com/apsyadira-jubelio/go-amqp-reconnect/rabbitmq v0.0.0-20240414094204-454afb52b150 => ../../rabbitmq
