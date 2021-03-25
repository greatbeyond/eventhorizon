// Copyright (c) 2015 - Max Ekman <max@looplab.se>
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

// Package mongodb is a eventhorizon read repository for mongodb
package mongodb

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	eh "github.com/looplab/eventhorizon"
)

// ErrCouldNotDialDB is when the database could not be dialed.
var ErrCouldNotDialDB = errors.New("could not dial database")

// ErrNoDBSession is when no database session is set.
var ErrNoDBSession = errors.New("no database session")

// ErrCouldNotClearDB is when the database could not be cleared.
var ErrCouldNotClearDB = errors.New("could not clear database")

// ErrModelNotSet is when an model factory is not set on the Repo.
var ErrModelNotSet = errors.New("model not set")

// ErrInvalidQuery is when a query was not returned from the callback to FindCustom.
var ErrInvalidQuery = errors.New("invalid query")

// Repo implements an MongoDB repository for entities.
type Repo struct {
	client     *mongo.Client
	dbPrefix   string
	collection string
	factoryFn  func() eh.Entity
}

// NewRepo creates a new Repo with a new mongo client connection.
func NewRepo(url, dbPrefix, collection string) (*Repo, error) {
	opts := options.Client().ApplyURI(url).SetReadPreference(readpref.Primary())
	client, err := mongo.Connect(context.TODO(), opts)
	if err != nil {
		return nil, ErrCouldNotDialDB
	}

	return NewRepoWithClient(client, dbPrefix, collection)
}

// NewRepoWithClient creates a new Repo with an existing mongo client.
func NewRepoWithClient(client *mongo.Client, dbPrefix, collection string) (*Repo, error) {
	if client == nil {
		return nil, ErrNoDBSession
	}

	r := &Repo{
		client:     client,
		dbPrefix:   dbPrefix,
		collection: collection,
	}

	return r, nil
}

// Parent implements the Parent method of the eventhorizon.ReadRepo interface.
func (r *Repo) Parent() eh.ReadRepo {
	return nil
}

// Find implements the Find method of the eventhorizon.ReadRepo interface.
func (r *Repo) Find(ctx context.Context, id eh.UUID) (eh.Entity, error) {
	if r.factoryFn == nil {
		return nil, eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	entity := r.factoryFn()
	err := r.client.Database(r.dbName(ctx)).Collection(r.collection).FindOne(
		ctx,
		bson.M{
			"_id": id,
		},
	).Decode(entity)
	if err != nil {
		return nil, eh.RepoError{
			Err:       eh.ErrEntityNotFound,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return entity, nil
}

// FindAll implements the FindAll method of the eventhorizon.ReadRepo interface.
func (r *Repo) FindAll(ctx context.Context) ([]eh.Entity, error) {
	if r.factoryFn == nil {
		return nil, eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	cursor, err := r.client.Database(r.dbName(ctx)).Collection(r.collection).Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	result := []eh.Entity{}
	entity := r.factoryFn()

	for cursor.Next(ctx) {
		err := cursor.Decode(entity)
		if err != nil {
			return nil, err
		}

		result = append(result, entity)
		entity = r.factoryFn()
	}

	if err := cursor.Close(ctx); err != nil {
		return nil, eh.RepoError{
			Err:       err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return result, nil
}

// The iterator is not thread safe.
type iter struct {
	client    *mongo.Client
	cursor    *mongo.Cursor
	factoryFn func() eh.Entity
	item      eh.Entity
	decodeErr error
}

func (i *iter) Next() bool {
	if i.cursor.Next(context.TODO()) {
		item := i.factoryFn()
		if err := i.cursor.Decode(item); err != nil {
			i.decodeErr = err
			i.item = nil
			return false
		}
		i.item = item

		return true
	}

	return false
}

func (i *iter) Value() interface{} {
	return i.item
}

func (i *iter) Close() error {
	err := i.cursor.Close(context.TODO())
	if err != nil {
		return err
	}

	if i.decodeErr != nil {
		return i.decodeErr
	}

	return nil
}

// FindCustomIter returns a cursor you can use to stream results of very large datasets
func (r *Repo) FindCustomIter(ctx context.Context, callback func(*mongo.Collection) (*mongo.Cursor, error)) (eh.Iter, error) {
	if r.factoryFn == nil {
		return nil, eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	collection := r.client.Database(r.dbName(ctx)).Collection(r.collection)
	cursor, err := callback(collection)
	if err != nil {
		return nil, eh.RepoError{
			BaseErr:   err,
			Err:       ErrInvalidQuery,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}
	if cursor == nil {
		return nil, eh.RepoError{
			Err:       ErrInvalidQuery,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return &iter{
		client:    r.client,
		cursor:    cursor,
		factoryFn: r.factoryFn,
	}, nil
}

// FindCustom uses a callback to specify a custom query for returning models.
// It can also be used to do queries that does not map to the model by executing
// the query in the callback and returning nil to block a second execution of
// the same query in FindCustom. Expect a ErrInvalidQuery if returning a nil
// query from the callback.
func (r *Repo) FindCustom(ctx context.Context, callback func(*mongo.Collection) (*mongo.Cursor, error)) ([]interface{}, error) {
	if r.factoryFn == nil {
		return nil, eh.RepoError{
			Err:       ErrModelNotSet,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	collection := r.client.Database(r.dbName(ctx)).Collection(r.collection)
	cursor, err := callback(collection)
	if err != nil {
		return nil, eh.RepoError{
			BaseErr:   err,
			Err:       ErrInvalidQuery,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}
	if cursor == nil {
		return nil, eh.RepoError{
			Err:       ErrInvalidQuery,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	result := []interface{}{}
	entity := r.factoryFn()
	for cursor.Next(ctx) {
		err := cursor.Decode(entity)
		if err != nil {
			return nil, err
		}

		result = append(result, entity)
		entity = r.factoryFn()
	}
	if err := cursor.Close(ctx); err != nil {
		return nil, eh.RepoError{
			Err:       err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return result, nil
}

// Save implements the Save method of the eventhorizon.WriteRepo interface.
func (r *Repo) Save(ctx context.Context, entity eh.Entity) error {
	if entity.EntityID() == "" {
		return eh.RepoError{
			Err:       eh.ErrCouldNotSaveEntity,
			BaseErr:   eh.ErrMissingEntityID,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	if _, err := r.client.Database(r.dbName(ctx)).Collection(r.collection).UpdateOne(
		ctx,
		bson.M{
			"_id": entity.EntityID(),
		},
		bson.M{
			"$set": entity,
		},
		options.Update().SetUpsert(true),
	); err != nil {
		return eh.RepoError{
			Err:       eh.ErrCouldNotSaveEntity,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}
	return nil
}

// Remove implements the Remove method of the eventhorizon.WriteRepo interface.
func (r *Repo) Remove(ctx context.Context, id eh.UUID) error {
	result, err := r.client.Database(r.dbName(ctx)).Collection(r.collection).DeleteOne(
		ctx,
		bson.M{
			"_id": id,
		},
	)
	if err != nil {
		return eh.RepoError{
			Err:       eh.ErrEntityNotFound,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	} else if result.DeletedCount == 0 {
		return eh.RepoError{
			Err:       eh.ErrEntityNotFound,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return nil
}

// Collection lets the function do custom actions on the collection.
func (r *Repo) Collection(ctx context.Context, f func(*mongo.Collection) error) error {
	c := r.client.Database(r.dbName(ctx)).Collection(r.collection)
	if err := f(c); err != nil {
		return eh.RepoError{
			Err:       err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}

	return nil
}

// SetEntityFactory sets a factory function that creates concrete entity types.
func (r *Repo) SetEntityFactory(f func() eh.Entity) {
	r.factoryFn = f
}

// Clear clears the read model database.
func (r *Repo) Clear(ctx context.Context) error {
	if err := r.client.Database(r.dbName(ctx)).Collection(r.collection).Drop(ctx); err != nil {
		return eh.RepoError{
			Err:       ErrCouldNotClearDB,
			BaseErr:   err,
			Namespace: eh.NamespaceFromContext(ctx),
		}
	}
	return nil
}

// Close closes a database session.
func (r *Repo) Close() {}

// dbName appends the namespace, if one is set, to the DB prefix to
// get the name of the DB to use.
func (r *Repo) dbName(ctx context.Context) string {
	ns := eh.NamespaceFromContext(ctx)
	return r.dbPrefix + "_" + ns
}

// Repository returns a parent ReadRepo if there is one.
func Repository(repo eh.ReadRepo) *Repo {
	if repo == nil {
		return nil
	}

	if r, ok := repo.(*Repo); ok {
		return r
	}

	return Repository(repo.Parent())
}
