// Copyright (c) 2014 - Max Ekman <max@looplab.se>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package redis

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/jpillora/backoff"
	"go.mongodb.org/mongo-driver/bson"

	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/publisher/local"
)

// ErrCouldNotMarshalEvent is when an event could not be marshaled into BSON.
var ErrCouldNotMarshalEvent = errors.New("could not marshal event")

// ErrCouldNotUnmarshalEvent is when an event could not be unmarshaled into a concrete type.
var ErrCouldNotUnmarshalEvent = errors.New("could not unmarshal event")

// EventPublisher is an event bus that notifies registered EventHandlers of
// published events. It will use the SimpleEventHandlingStrategy by default.
type EventPublisher struct {
	*local.EventPublisher

	prefix string
	pool   *redis.Pool
	conn   *redis.PubSubConn
	ready  chan bool // NOTE: Used for testing only
	exit   chan bool
}

// NewEventPublisher creates a EventPublisher for remote events.
func NewEventPublisher(appID, server, password string) (*EventPublisher, error) {
	pool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}

	return NewEventPublisherWithPool(appID, pool)
}

// NewEventPublisherWithPool creates a EventPublisher for remote events.
func NewEventPublisherWithPool(appID string, pool *redis.Pool) (*EventPublisher, error) {
	b := &EventPublisher{
		EventPublisher: local.NewEventPublisher(),
		prefix:         appID + ":events:",
		pool:           pool,
		ready:          make(chan bool, 1), // Buffered to not block receive loop.
		exit:           make(chan bool),
	}

	go func() {
		log.Println("eventpublisher: start receiving")
		defer log.Println("eventpublisher: stop receiving")

		// Used for exponential fall back on reconnects.
		delay := &backoff.Backoff{
			Max: 5 * time.Minute,
		}

		for {
			if err := b.recv(delay); err != nil {
				d := delay.Duration()
				log.Printf("eventpublisher: receive failed, retrying in %s: %s", d, err)
				time.Sleep(d)
				continue
			}

			return
		}
	}()

	return b, nil
}

// PublishEvent publishes an event to all handlers capable of handling it.
func (b *EventPublisher) PublishEvent(ctx context.Context, event eh.Event) error {
	e := redisEvent{
		AggregateID:   event.AggregateID(),
		AggregateType: event.AggregateType(),
		EventType:     event.EventType(),
		Version:       event.Version(),
		Timestamp:     event.Timestamp(),
		Context:       eh.MarshalContext(ctx),
	}

	// Marshal event data if there is any.
	if event.Data() != nil {
		var err error
		if e.RawData, err = bson.Marshal(event.Data()); err != nil {
			return ErrCouldNotMarshalEvent
		}
	}

	// Marshal the event (using BSON for now).
	data, err := bson.Marshal(e)
	if err != nil {
		return ErrCouldNotMarshalEvent
	}

	conn := b.pool.Get()
	defer conn.Close()

	if err := conn.Err(); err != nil {
		return err
	}

	// Publish all events on their own channel.
	if _, err = conn.Do("PUBLISH", b.prefix+string(event.EventType()), data); err != nil {
		return err
	}

	return nil
}

// Close exits the receive goroutine by unsubscribing to all channels.
func (b *EventPublisher) Close() error {
	select {
	case b.exit <- true:
	default:
		log.Println("eventpublisher: already closed")
	}

	return b.pool.Close()
}

func (b *EventPublisher) recv(delay *backoff.Backoff) error {
	conn := b.pool.Get()
	defer conn.Close()

	pubSubConn := &redis.PubSubConn{Conn: conn}
	go func() {
		<-b.exit
		if err := pubSubConn.PUnsubscribe(); err != nil {
			log.Println("eventpublisher: could not unsubscribe:", err)
		}
		if err := pubSubConn.Close(); err != nil {
			log.Println("eventpublisher: could not close connection:", err)
		}
	}()

	err := pubSubConn.PSubscribe(b.prefix + "*")
	if err != nil {
		return err
	}

	for {
		switch m := pubSubConn.Receive().(type) {
		case redis.PMessage:
			if err := b.handleMessage(m); err != nil {
				log.Println("eventpublisher: error publishing:", err)
			}

		case redis.Subscription:
			if m.Kind == "psubscribe" {
				log.Println("eventpublisher: subscribed to:", m.Channel)
				delay.Reset()

				// Don't block if no one is receiving and buffer is full.
				select {
				case b.ready <- true:
				default:
				}
			}
		case error:
			// Don' treat connections closed by the user as errors.
			if m.Error() == "redigo: get on closed pool" ||
				m.Error() == "redigo: connection closed" {
				return nil
			}

			return m
		}
	}
}

func (b *EventPublisher) handleMessage(msg redis.PMessage) error {
	// Decode the raw BSON event data.
	var e redisEvent
	if err := bson.Unmarshal(msg.Data, &e); err != nil {
		// TODO: Forward the real error.
		return ErrCouldNotUnmarshalEvent
	}

	// Create an event of the correct type and decode from raw BSON.
	if len(e.RawData) > 0 {
		var err error
		if e.data, err = eh.CreateEventData(e.EventType); err != nil {
			// TODO: Forward the real error.
			return ErrCouldNotUnmarshalEvent
		}

		if err := bson.Unmarshal(e.RawData, e.data); err != nil {
			// TODO: Forward the real error.
			return ErrCouldNotUnmarshalEvent
		}
		e.RawData = nil
	}

	event := event{redisEvent: e}
	ctx := eh.UnmarshalContext(e.Context)

	// Notify all observers about the event.
	return b.EventPublisher.PublishEvent(ctx, event)
}

// redisEvent is the internal event used with the Redis event bus.
type redisEvent struct {
	EventType     eh.EventType           `bson:"event_type"`
	RawData       bson.Raw               `bson:"data,omitempty"`
	data          eh.EventData           `bson:"-"`
	Timestamp     time.Time              `bson:"timestamp"`
	AggregateType eh.AggregateType       `bson:"aggregate_type"`
	AggregateID   eh.UUID                `bson:"_id"`
	Version       int                    `bson:"version"`
	Context       map[string]interface{} `bson:"context"`
}

// event is the private implementation of the eventhorizon.Event interface
// for a MongoDB event store.
type event struct {
	redisEvent
}

// EventType implements the EventType method of the eventhorizon.Event interface.
func (e event) EventType() eh.EventType {
	return e.redisEvent.EventType
}

// Data implements the Data method of the eventhorizon.Event interface.
func (e event) Data() eh.EventData {
	return e.redisEvent.data
}

// Timestamp implements the Timestamp method of the eventhorizon.Event interface.
func (e event) Timestamp() time.Time {
	return e.redisEvent.Timestamp
}

// AggregateType implements the AggregateType method of the eventhorizon.Event interface.
func (e event) AggregateType() eh.AggregateType {
	return e.redisEvent.AggregateType
}

// AggrgateID implements the AggrgateID method of the eventhorizon.Event interface.
func (e event) AggregateID() eh.UUID {
	return e.redisEvent.AggregateID
}

// Version implements the Version method of the eventhorizon.Event interface.
func (e event) Version() int {
	return e.redisEvent.Version
}

// String implements the String method of the eventhorizon.Event interface.
func (e event) String() string {
	return fmt.Sprintf("%s@%d", e.redisEvent.EventType, e.redisEvent.Version)
}
