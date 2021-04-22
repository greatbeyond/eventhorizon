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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
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

	streamName  string
	client      *redis.Client
	clientOpts  *redis.Options
	errCh       chan error
	parallel    int
	ackDeadline time.Duration
}

// NewEventPublisher creates a EventPublisher for remote events.
func NewEventPublisher(addr, appID, subscriberID string, options ...Option) (*EventPublisher, error) {
	b := &EventPublisher{
		EventPublisher: local.NewEventPublisher(),
		streamName:     appID + "_events",
		errCh:          make(chan error, 20),
		parallel:       10,
		ackDeadline:    60 * time.Second,
	}

	// Apply configuration options.
	for _, option := range options {
		if option == nil {
			continue
		}
		if err := option(b); err != nil {
			return nil, fmt.Errorf("error while applying option: %w", err)
		}
	}

	// Default client options.
	if b.clientOpts == nil {
		b.clientOpts = &redis.Options{
			Addr: addr,
		}
	}

	ctx := context.Background()

	// Create client and check connection.
	b.client = redis.NewClient(b.clientOpts)
	if res, err := b.client.Ping(ctx).Result(); err != nil || res != "PONG" {
		return nil, fmt.Errorf("could not check Redis server: %w", err)
	}

	groupName := appID + "_" + subscriberID
	res, err := b.client.XGroupCreateMkStream(ctx, b.streamName, groupName, "$").Result()
	if err != nil {
		// Ignore group exists non-errors.
		if !strings.HasPrefix(err.Error(), "BUSYGROUP") {
			return nil, fmt.Errorf("could not create consumer group: %w", err)
		}
	} else if res != "OK" {
		return nil, fmt.Errorf("could not create consumer group: %s", res)
	}

	go func() {
		if err := b.recv(context.Background(), groupName); err != context.Canceled {
			select {
			case b.errCh <- Error{Err: errors.New("could not receive"), BaseErr: err}:
			default:
			}
		}
	}()

	return b, nil
}

// Option is an option setter used to configure creation.
type Option func(*EventPublisher) error

// WithRedisOptions uses the Redis options for the underlying client, instead of the defaults.
func WithRedisOptions(opts *redis.Options) Option {
	return func(b *EventPublisher) error {
		b.clientOpts = opts
		return nil
	}
}

const dataKey = "data"

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

	args := &redis.XAddArgs{
		Stream: b.streamName,
		Values: map[string]interface{}{
			dataKey: data,
		},
	}
	if _, err := b.client.XAdd(ctx, args).Result(); err != nil {
		return fmt.Errorf("could not publish event: %w", err)
	}

	return nil
}

func (b *EventPublisher) recv(ctx context.Context, groupName string) error {
	queue := make(chan *redis.XMessage)
	wg := sync.WaitGroup{}
	for i := 0; i < b.parallel; i++ {
		wg.Add(1)
		go func(ch <-chan *redis.XMessage) {
			for msg := range ch {
				// TODO: Maybe add ack deadline timeout here. Send to nack channel.
				b.handleMessage(ctx, msg, groupName)
			}
			wg.Done()
		}(queue)
	}
	// TODO: Close queue and wait for goroutines to close.

	readOpt := ">"
	for {
		streams, err := b.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: groupName,
			Streams:  []string{b.streamName, readOpt},
		}).Result()
		if err != nil {
			return err
		}

		// Check resulting streams to match our single stream.
		if len(streams) != 1 || streams[0].Stream != b.streamName {
			continue
		}
		stream := streams[0]

		// Handle all messages from group read.
		for _, msg := range stream.Messages {
			publishTime := timeFromRedisID(msg.ID)
			if readOpt == "0" && publishTime.After(time.Now().Add(-b.ackDeadline)) {
				// Ignore messages which have not yet reach their ack deadline.
				continue
			}

			// Block until previous messages are handeled.
			queue <- &msg
		}

		// Flip flop the read option to read new and non-acked messages every other time.
		if readOpt == ">" {
			readOpt = "0"
		} else {
			readOpt = ">"
		}
	}
}

func (b *EventPublisher) handleMessage(ctx context.Context, msg *redis.XMessage, groupName string) {
	// Ack directly after receiving the event to avoid getting retries.
	// TODO: Figure out when to best ack/nack a message.
	if _, err := b.client.XAck(ctx, b.streamName, groupName, msg.ID).Result(); err != nil {
		select {
		case b.errCh <- Error{Err: fmt.Errorf("could not ack event: %w", err)}:
		default:
		}
		return
	}

	data, ok := msg.Values[dataKey].(string)
	if !ok {
		select {
		case b.errCh <- Error{Err: ErrCouldNotUnmarshalEvent}:
		default:
		}
		return
	}

	// Decode the raw BSON event data.
	var e redisEvent
	if err := bson.Unmarshal([]byte(data), &e); err != nil {
		select {
		case b.errCh <- Error{Err: ErrCouldNotUnmarshalEvent, BaseErr: err}:
		default:
		}
		return
	}

	// Create an event of the correct type and decode from raw BSON.
	if len(e.RawData) > 0 {
		var err error
		if e.data, err = eh.CreateEventData(e.EventType); err != nil {
			select {
			case b.errCh <- Error{Err: ErrCouldNotUnmarshalEvent, BaseErr: fmt.Errorf("%s: %s, %s, %s", err.Error(), e.AggregateType, e.EventType, e.AggregateID)}:
			default:
			}
			return
		}

		if err := bson.Unmarshal(e.RawData, e.data); err != nil {
			select {
			case b.errCh <- Error{Err: ErrCouldNotUnmarshalEvent, BaseErr: fmt.Errorf("%s: %s, %s, %s", err.Error(), e.AggregateType, e.EventType, e.AggregateID)}:
			default:
			}
			return
		}
		e.RawData = nil
	}

	event := event{redisEvent: e}
	ctx = eh.UnmarshalContext(e.Context)

	// Notify all observers about the event.
	if err := b.EventPublisher.PublishEvent(ctx, event); err != nil {
		select {
		case b.errCh <- Error{Ctx: ctx, Err: err, Event: event}:
		default:
		}
		return
	}
}

// Errors returns an error channel where async handling errors are sent.
func (b *EventPublisher) Errors() <-chan error {
	return b.errCh
}

// Error is an async error containing the error and the event.
type Error struct {
	Err     error
	BaseErr error
	Ctx     context.Context
	Event   eh.Event
}

// Error implements the Error method of the error interface.
func (e Error) Error() string {
	if e.Event != nil {
		return fmt.Sprintf("%s: %s (%s)", e.Event.String(), e.Err, e.BaseErr)
	}
	return fmt.Sprintf("%s (%s)", e.Err, e.BaseErr)
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

// Converts a Redis message ID in format "1526919030474-55" to time.Time.
// Both quantities are 64-bit numbers. When an ID is auto-generated, the first
// part is the Unix time in milliseconds of the Redis instance generating the ID.
// The second part is just a sequence number and is used in order to distinguish
// IDs generated in the same millisecond.
// See: https://redis.io/commands/xadd#specifying-a-stream-id-as-an-argument
func timeFromRedisID(id string) time.Time {
	parts := strings.Split(id, "-")
	if len(parts) == 0 {
		return time.Time{}
	}
	ms, err := strconv.Atoi(parts[0])
	if err != nil {
		return time.Time{}
	}
	s := int64(ms) / 1e3
	ns := (int64(ms) - s*1e3) * 1e6
	return time.Unix(s, ns)
}
