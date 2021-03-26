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

package mongodb

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	eh "github.com/looplab/eventhorizon"
	"github.com/looplab/eventhorizon/mocks"
	"github.com/looplab/eventhorizon/repo/testutil"
)

func TestReadRepo(t *testing.T) {
	// Use MongoDB in Docker with fallback to localhost.
	addr := os.Getenv("MONGODB_ADDR")
	if addr == "" {
		addr = "localhost:27017"
	}
	url := "mongodb://" + addr

	repo, err := NewRepo(url, "test", "mocks.Model")
	if err != nil {
		t.Error("there should be no error:", err)
	}
	if repo == nil {
		t.Error("there should be a repository")
	}
	defer repo.Close()
	repo.SetEntityFactory(func() eh.Entity {
		return &mocks.Model{}
	})
	if repo.Parent() != nil {
		t.Error("the parent repo should be nil")
	}

	// Repo with default namespace.
	defer func() {
		t.Log("clearing default db")
		if err = repo.Clear(context.Background()); err != nil {
			t.Fatal("there should be no error:", err)
		}
	}()
	testutil.RepoCommonTests(t, context.Background(), repo)
	extraRepoTests(t, context.Background(), repo)

	// Repo with other namespace.
	ctx := eh.NewContextWithNamespace(context.Background(), "ns")
	defer func() {
		t.Log("clearing ns db")
		if err = repo.Clear(ctx); err != nil {
			t.Fatal("there should be no error:", err)
		}
	}()
	testutil.RepoCommonTests(t, ctx, repo)
	extraRepoTests(t, ctx, repo)

}

func extraRepoTests(t *testing.T, ctx context.Context, repo *Repo) {
	// Insert a custom item.
	modelCustom := &mocks.Model{
		ID:        eh.NewUUID(),
		Content:   "modelCustom",
		CreatedAt: time.Now().UTC().Round(time.Millisecond),
	}
	if err := repo.Save(ctx, modelCustom); err != nil {
		t.Error("there should be no error:", err)
	}

	// FindCustom by content.
	result, err := repo.FindCustom(ctx, func(c *mongo.Collection) (*mongo.Cursor, error) {
		return c.Find(ctx, bson.M{"content": "modelCustom"})
	})
	if len(result) != 1 {
		t.Error("there should be one item:", len(result))
	}
	if !reflect.DeepEqual(result[0], modelCustom) {
		t.Error("the item should be correct:", modelCustom)
	}

	// FindCustom with no query.
	_, err = repo.FindCustom(ctx, func(c *mongo.Collection) (*mongo.Cursor, error) {
		return nil, nil
	})
	if rrErr, ok := err.(eh.RepoError); !ok || rrErr.Err != ErrInvalidQuery {
		t.Error("there should be a invalid query error:", err)
	}

	var count int64
	// FindCustom with query execution in the callback.
	_, err = repo.FindCustom(ctx, func(c *mongo.Collection) (*mongo.Cursor, error) {
		if count, err = c.CountDocuments(ctx, bson.M{}); err != nil {
			t.Error("there should be no error:", err)
		}

		// Be sure to return nil to not execute the query again in FindCustom.
		return nil, nil
	})
	if rrErr, ok := err.(eh.RepoError); !ok || rrErr.Err != ErrInvalidQuery {
		t.Error("there should be a invalid query error:", err)
	}
	if count != 2 {
		t.Error("the count should be correct:", count)
	}

	modelCustom2 := &mocks.Model{
		ID:      eh.NewUUID(),
		Content: "modelCustom2",
	}
	if err := repo.Collection(ctx, func(c *mongo.Collection) error {
		_, err := c.InsertOne(ctx, modelCustom2)
		return err
	}); err != nil {
		t.Error("there should be no error:", err)
	}
	model, err := repo.Find(ctx, modelCustom2.ID)
	if err != nil {
		t.Error("there should be no error:", err)
	}
	if !reflect.DeepEqual(model, modelCustom2) {
		t.Error("the item should be correct:", model)
	}

	// FindCustomIter by content.
	iter, err := repo.FindCustomIter(ctx, func(c *mongo.Collection) (*mongo.Cursor, error) {
		return c.Find(ctx, bson.M{"content": "modelCustom"})
	})
	if err != nil {
		t.Error("there should be no error:", err)
	}

	if iter.Next() != true {
		t.Error("the iterator should have results")
	}
	if !reflect.DeepEqual(iter.Value(), modelCustom) {
		t.Error("the item should be correct:", modelCustom)
	}
	if iter.Next() == true {
		t.Error("the iterator should have no results")
	}
	err = iter.Close()
	if err != nil {
		t.Error("there should be no error:", err)
	}

}

func TestRepository(t *testing.T) {
	if r := Repository(nil); r != nil {
		t.Error("the parent repository should be nil:", r)
	}

	inner := &mocks.Repo{}
	if r := Repository(inner); r != nil {
		t.Error("the parent repository should be nil:", r)
	}

	// Use MongoDB in Docker with fallback to localhost.
	addr := os.Getenv("MONGODB_ADDR")
	if addr == "" {
		addr = "localhost:27017"
	}
	url := "mongodb://" + addr

	repo, err := NewRepo(url, "test", "mocks.Model")
	if err != nil {
		t.Error("there should be no error:", err)
	}
	defer repo.Close()

	outer := &mocks.Repo{ParentRepo: repo}
	if r := Repository(outer); r != repo {
		t.Error("the parent repository should be correct:", r)
	}
}
