package database

import (
	"context"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	mongoURI       string = "mongodb://localhost:27017"
	pingEnabled    bool   = true
	timeout               = 10 * time.Second
	databaseName   string = "mydb"
	collectionName string = "btc1mData"
)

func Connect(ctx context.Context) *mongo.Collection {
	// Set client options
	clientOptions := options.Client().ApplyURI(mongoURI).
		SetConnectTimeout(timeout).
		SetSocketTimeout(timeout)

	// Connect to MongoDB
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		panic(err)
	}

	if pingEnabled {
		// Check the connection
		err = client.Ping(ctx, nil)
		if err != nil {
			panic(err)
		}
	}

	log.Printf("Connected to MongoDB!")

	// Set up a context with a timeout and defer the cancel function
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Connect to the MongoDB server and defer the disconnection
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	// Get a handle to the database
	db := client.Database(databaseName)

	// Set up the options for the time-series collection
	options := bson.D{
		primitive.E{Key: "create", Value: collectionName},
		primitive.E{Key: "timeseries", Value: bson.D{
			primitive.E{Key: "timeField", Value: "timestamp"},
			primitive.E{Key: "metaField", Value: "metadata"},
		}},
	}

	// Run the create command
	result := db.RunCommand(ctx, options)

	// Check for any errors
	if err := result.Err(); err != nil {
		log.Fatal(err)
	}

	// Get a handle to the collection
	collection := db.Collection(collectionName)

	return collection
}
