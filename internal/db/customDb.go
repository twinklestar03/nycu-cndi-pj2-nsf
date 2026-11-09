package db

import (
	"context"
	"errors"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

const mongoTimeout = 5 * time.Second

type mongoDb struct {
	client     *mongo.Client
	collection *mongo.Collection
}

type mongoEntry struct {
	Key   string `bson:"_id"`
	Value string `bson:"value"`
}

var _ DbIf = (*mongoDb)(nil)

func newMongoDb() (*mongoDb, error) {
	uri := os.Getenv("NSF_MONGO_URI")
	if uri == "" {
		uri = "mongodb://127.0.0.1:27017"
	}
	database := os.Getenv("NSF_MONGO_DATABASE")
	if database == "" {
		database = "nsf"
	}
	client, err := mongo.Connect(options.Client().ApplyURI(uri).SetServerSelectionTimeout(mongoTimeout))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), mongoTimeout)
	defer cancel()
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}
	return &mongoDb{client: client, collection: client.Database(database).Collection("services")}, nil
}

func (m *mongoDb) Release() error {
	ctx, cancel := context.WithTimeout(context.Background(), mongoTimeout)
	defer cancel()
	return m.client.Disconnect(ctx)
}

func (m *mongoDb) Save(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), mongoTimeout)
	defer cancel()
	_, err := m.collection.ReplaceOne(ctx, bson.M{"_id": key}, mongoEntry{key, value}, options.Replace().SetUpsert(true))
	return err
}

func (m *mongoDb) SaveAll(data map[string]string) error {
	if len(data) == 0 {
		return nil
	}
	models := make([]mongo.WriteModel, 0, len(data))
	for key, value := range data {
		models = append(models, mongo.NewReplaceOneModel().SetFilter(bson.M{"_id": key}).SetReplacement(mongoEntry{key, value}).SetUpsert(true))
	}
	ctx, cancel := context.WithTimeout(context.Background(), mongoTimeout)
	defer cancel()
	_, err := m.collection.BulkWrite(ctx, models)
	return err
}

func (m *mongoDb) Exist(key string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), mongoTimeout)
	defer cancel()
	err := m.collection.FindOne(ctx, bson.M{"_id": key}).Err()
	if errors.Is(err, mongo.ErrNoDocuments) {
		return false, nil
	}
	return err == nil, err
}

func (m *mongoDb) Load(key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), mongoTimeout)
	defer cancel()
	var entry mongoEntry
	err := m.collection.FindOne(ctx, bson.M{"_id": key}).Decode(&entry)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return "", nil
	}
	return entry.Value, err
}

func (m *mongoDb) LoadAll() (map[string]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), mongoTimeout)
	defer cancel()
	cursor, err := m.collection.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = cursor.Close(ctx) }()
	var entries []mongoEntry
	if err := cursor.All(ctx, &entries); err != nil {
		return nil, err
	}
	data := make(map[string]string, len(entries))
	for _, entry := range entries {
		data[entry.Key] = entry.Value
	}
	return data, nil
}

func (m *mongoDb) Remove(key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), mongoTimeout)
	defer cancel()
	_, err := m.collection.DeleteOne(ctx, bson.M{"_id": key})
	return err
}

func (m *mongoDb) RemoveAll() error {
	ctx, cancel := context.WithTimeout(context.Background(), mongoTimeout)
	defer cancel()
	_, err := m.collection.DeleteMany(ctx, bson.M{})
	return err
}
