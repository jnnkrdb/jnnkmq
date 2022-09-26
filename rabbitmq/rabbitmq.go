package rabbitmq

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"time"

	"github.com/jnnkrdb/corerdb/prtcl"
	amqp "github.com/rabbitmq/amqp091-go"
)

// config collection for the message queue
//
// can be inserted via json-string
//
//	{
//		"username": "",
//		"password": "",
//		"address": "",
//		"port": ""
//	}
type RabbitMQ struct {
	// user connected to the rabbitq instance
	Username string `json:"username"`
	// password of the connected user, base64 encoded
	Password string `json:"password"`
	// address of the rabbitmq instance as uri, the protocol is [amqp://]
	Address string `json:"address"`
	// port on which the rabbitmq instance listens
	Port string `json:"port"`

	// runtime vars
	endpoint *amqp.Connection
	channel  *amqp.Channel
	queue    amqp.Queue
}

// load a configuration from a file
//
// Parameters:
//   - `path` : string > path to the jsonfile, which contains the settings
func LoadRabbitMQ(path string) (RabbitMQ, error) {

	prtcl.Log.Println("loading rabbitmq-auth configuration from", path)

	var rmq = RabbitMQ{}

	if jsonf, err := os.ReadFile(path); err == nil {

		if err := json.Unmarshal(jsonf, &rmq); err != nil {

			prtcl.PrintObject(jsonf, rmq, err)

			return rmq, err
		}

	} else {

		prtcl.PrintObject(jsonf, rmq, err)

		return rmq, err
	}

	return rmq, nil
}

// decode the base64 password
func (rmq RabbitMQ) UnencodedPassword() string {

	if str, err := base64.StdEncoding.DecodeString(rmq.Password); err == nil {

		return string(str)
	}

	return ""
}

// --------------------------------------------------------------
// functions about the rmq instance

// connect to the rmq-endpoint
func (rmq *RabbitMQ) Connect() error {

	prtcl.Log.Println("connecting to rabbitmq-instance:", rmq.Address+":"+rmq.Port)

	if rmqserver, err := amqp.Dial("amqp://" + rmq.Username + ":" + rmq.UnencodedPassword() + "@" + rmq.Address + ":" + rmq.Port + "/"); err != nil {

		prtcl.PrintObject(rmq, rmqserver, err)

		return err

	} else {

		rmq.endpoint = rmqserver

		if ch, err := rmq.endpoint.Channel(); err != nil {

			prtcl.PrintObject(rmq, rmqserver, ch, err)

			return err

		} else {

			rmq.channel = ch

			return nil
		}
	}
}

// get the connection
func (rmq RabbitMQ) Connection() *amqp.Connection {

	return rmq.endpoint
}

// get the channel
func (rmq RabbitMQ) Channel() *amqp.Channel {

	return rmq.channel
}

// disconnect from the rmq instance
func (rmq *RabbitMQ) Disconnect() error {

	prtcl.Log.Println("disconnecting from rabbitmq-instance:", rmq.Address+":"+rmq.Port)

	// first close current channel
	if err := rmq.channel.Close(); err != nil {

		prtcl.PrintObject(rmq, err)

		return err
	}

	// close the current connection
	if err := rmq.endpoint.Close(); err != nil {

		prtcl.PrintObject(rmq, err)

		return err
	}

	return nil
}

// --------------------------------------------------------------
// queue functions

// create a queue for the rmq instance
//
// Parameters:
//   - `qc` : QueueConfig > configure the queue with an struct, readable via json
func (rmq *RabbitMQ) InitQueue(qc QueueDefinition) error {

	if queue, err := rmq.Channel().QueueDeclare(
		qc.Name,
		qc.Durable,
		qc.AutoDelete,
		qc.Exclusiv,
		qc.NoWait,
		nil,
	); err != nil {

		prtcl.Log.Println("a problem occured while creating a queue for the current channel")

		prtcl.PrintObject(rmq, qc, queue, err)

		return err

	} else {

		rmq.queue = queue

		return nil
	}
}

// send a message to the configured queue
//
// Parameters:
//   - `message` : string > send a message to the configured queue
func (rmq *RabbitMQ) Send(message string) error {

	// create the background context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	publishing := amqp.Publishing{
		ContentType: "text/plain",
		Body:        []byte(message),
	}

	//send the actual msg
	if err := rmq.Channel().PublishWithContext(
		ctx,
		"",
		rmq.queue.Name,
		false,
		false,
		publishing,
	); err != nil {

		prtcl.Log.Println("a problem occured while sending a msg to rabbitmq")

		prtcl.PrintObject(rmq, ctx, cancel, publishing, err)

		return err
	}

	return nil
}

// this function starts a goroutine and hovers over the messages from the queue.
// the messages will be send through the given channel. To process them you have do start another goroutine as a loop,
// so the received values can be processed seperatly.
// You can create as many goroutines, receiving from the given channel as you wish
//
// Parameters:
//   - `cstring` : chan string > string-typed chan, which receives the values of the received messages from the queue
func (rmq *RabbitMQ) Receive(cstring chan string) error {

	if msgs, err := rmq.Channel().Consume(
		rmq.queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {

		prtcl.PrintObject(rmq, cstring, msgs, err)

		return err

	} else {

		go func() {

			for d := range msgs {

				// only return the msg, don't print it to prtcl.Log
				//	prtcl.PrintObject(d.Body)

				cstring <- string(d.Body)
			}
		}()

		return nil
	}
}
