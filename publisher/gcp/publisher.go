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

// Package gco is a eventhorizon event publisher for GCP
package gcp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
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

	client *pubsub.Client
	topic  *pubsub.Topic
	errCh  chan error
}

// NewEventPublisher creates a EventPublisher.
func NewEventPublisher(projectID, appID, subscriberID string) (*EventPublisher, error) {
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}

	// Get or create the topic.
	name := appID + "_events"
	topic := client.Topic(name)
	if ok, err := topic.Exists(ctx); err != nil {
		return nil, err
	} else if !ok {
		if topic, err = client.CreateTopic(ctx, name); err != nil {
			return nil, err
		}
	}

	// Get or create the subscription.
	subscriptionID := appID + "_" + subscriberID
	sub := client.Subscription(subscriptionID)
	if ok, err := sub.Exists(ctx); err != nil {
		return nil, err
	} else if !ok {
		if sub, err = client.CreateSubscription(ctx, subscriptionID,
			pubsub.SubscriptionConfig{
				Topic: topic,
			},
		); err != nil {
			return nil, err
		}
	}

	b := &EventPublisher{
		EventPublisher: local.NewEventPublisher(),
		client:         client,
		topic:          topic,
		errCh:          make(chan error, 20),
	}

	go func() {
		for {
			if err := sub.Receive(ctx, b.handleMessage); err != context.Canceled {
				select {
				case b.errCh <- Error{Err: errors.New("could not receive"), BaseErr: err}:
				default:
					fmt.Printf("eventhorizon: could not log receive error: %s", err)
				}
			}
		}
	}()

	return b, nil
}

// PublishEvent implements the PublishEvent method of the eventhorizon.EventPublisher interface.
func (b *EventPublisher) PublishEvent(ctx context.Context, event eh.Event) error {
	e := gcpEvent{
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

	r := b.topic.Publish(ctx, &pubsub.Message{
		Data: data,
	})
	if _, err := r.Get(ctx); err != nil {
		return err
	}

	return nil
}

func (b *EventPublisher) handleMessage(ctx context.Context, msg *pubsub.Message) {
	// Ack directly after receiving the event to avoid getting retries.
	// TODO: Figure out when to best ack/nack a message.
	msg.Ack()

	// Manually decode the raw BSON event.
	var e gcpEvent
	if err := bson.Unmarshal(msg.Data, &e); err != nil {
		b.sendError(Error{Err: ErrCouldNotUnmarshalEvent, BaseErr: err})
		return
	}

	// Unmarshal event data with correct type, if there is data.
	if len(e.RawData) > 0 {
		var err error
		e.data, err = eh.CreateEventData(e.EventType)
		if err != nil {
			b.sendError(Error{
				Err:     ErrCouldNotUnmarshalEvent,
				BaseErr: fmt.Errorf("%s: %s, %s, %s", err.Error(), e.AggregateType, e.EventType, e.AggregateID),
			})
			return
		}

		if err := bson.Unmarshal(e.RawData, e.data); err != nil {
			b.sendError(Error{
				Err:     ErrCouldNotUnmarshalEvent,
				BaseErr: fmt.Errorf("%s: %s, %s, %s", err.Error(), e.AggregateType, e.EventType, e.AggregateID),
			})
			return
		}
		e.RawData = nil
	}

	event := event{gcpEvent: e}
	ctx = eh.UnmarshalContext(e.Context)

	// Notify all observers about the event.
	if err := b.EventPublisher.PublishEvent(ctx, event); err != nil {
		b.sendError(Error{Ctx: ctx, Err: err, Event: event})
	}
}

func (b *EventPublisher) sendError(err error) {
	select {
	case b.errCh <- err:
	default:
		fmt.Printf("eventhorizon: could not log error: %s", err)
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

// gcpEvent is the internal event used with the gcp event bus.
type gcpEvent struct {
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
	gcpEvent
}

// EventType implements the EventType method of the eventhorizon.Event interface.
func (e event) EventType() eh.EventType {
	return e.gcpEvent.EventType
}

// Data implements the Data method of the eventhorizon.Event interface.
func (e event) Data() eh.EventData {
	return e.gcpEvent.data
}

// Timestamp implements the Timestamp method of the eventhorizon.Event interface.
func (e event) Timestamp() time.Time {
	return e.gcpEvent.Timestamp
}

// AggregateType implements the AggregateType method of the eventhorizon.Event interface.
func (e event) AggregateType() eh.AggregateType {
	return e.gcpEvent.AggregateType
}

// AggrgateID implements the AggrgateID method of the eventhorizon.Event interface.
func (e event) AggregateID() eh.UUID {
	return e.gcpEvent.AggregateID
}

// Version implements the Version method of the eventhorizon.Event interface.
func (e event) Version() int {
	return e.gcpEvent.Version
}

// String implements the String method of the eventhorizon.Event interface.
func (e event) String() string {
	return fmt.Sprintf("%s@%d", e.gcpEvent.EventType, e.gcpEvent.Version)
}
