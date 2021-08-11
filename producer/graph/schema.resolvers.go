package graph

// This file will be automatically regenerated based on the schema, any resolver implementations
// will be copied through when generating and any unknown code will be moved to the end.

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"producer/graph/generated"
	"producer/graph/model"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func (r *mutationResolver) RegisterKafkaEvent(ctx context.Context, event model.RegisterKafkaEventInput) (*model.Event, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	topic := event.EventType
	CreateTopic(topic)
	currentTimeStamp := fmt.Sprintf("%v", time.Now().Unix())

	e := model.Event{
		ID:        currentTimeStamp,
		EventType: &event.EventType,
		Path:      &event.Path,
		Search:    &event.Search,
		Title:     &event.Title,
		UserID:    &event.UserID,
		URL:       &event.URL,
	}
	value, err := json.Marshal(e)
	if err != nil {
		log.Println("=> error converting event object to bytes:", err)
	}
	p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(value),
	}, nil)

	// Wait for message deliveries before shutting down
	p.Flush(15 * 1000)

	return &e, nil
}

func (r *queryResolver) Ping(ctx context.Context) (*model.PingResponse, error) {
	res := &model.PingResponse{
		Message: "Hello world",
	}
	return res, nil
}

// Mutation returns generated.MutationResolver implementation.
func (r *Resolver) Mutation() generated.MutationResolver { return &mutationResolver{r} }

// Query returns generated.QueryResolver implementation.
func (r *Resolver) Query() generated.QueryResolver { return &queryResolver{r} }

type (
	mutationResolver struct{ *Resolver }
	queryResolver    struct{ *Resolver }
)

// !!! WARNING !!!
// The code below was going to be deleted when updating resolvers. It has been copied here so you have
// one last chance to move it out of harms way if you want. There are two reasons this happens:
//  - When renaming or deleting a resolver the old code will be put in here. You can safely delete
//    it when you're done.
//  - You have helper methods in this file. Move them out to keep these resolver files clean.
func CreateTopic(topicName string) {
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": "localhost"})
	if err != nil {
		panic(err)
	}

	defer a.Close()

	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		panic("ParseDuration(60s)")
	}

	ctx := context.Background()
	results, err := a.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic: topicName,
		}},
		// Admin options
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		log.Printf("Failed to create topic: %v\n", err)
	}

	log.Println("results:", results)
}
