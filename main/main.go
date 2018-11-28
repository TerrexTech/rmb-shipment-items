package main

import (
	"log"
	"os"

	"github.com/TerrexTech/rmb-shipment-items/model"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/TerrexTech/go-eventspoll/poll"
	"github.com/TerrexTech/rmb-shipment-items/connutil"
	"github.com/TerrexTech/rmb-shipment-items/event"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
)

func validateEnv() error {
	log.Println("Reading environment file")
	err := godotenv.Load("./.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"SERVICE_NAME",

		"KAFKA_BROKERS",

		"KAFKA_CONSUMER_EVENT_GROUP",
		"KAFKA_CONSUMER_EVENT_QUERY_GROUP",

		"KAFKA_CONSUMER_EVENT_TOPIC",
		"KAFKA_CONSUMER_EVENT_QUERY_TOPIC",

		"KAFKA_PRODUCER_EVENT_QUERY_TOPIC",

		"KAFKA_END_OF_STREAM_TOKEN",

		"MONGO_HOSTS",
		"MONGO_DATABASE",
		"MONGO_AGG_COLLECTION",
		"MONGO_META_COLLECTION",
		"MONGO_CONNECTION_TIMEOUT_MS",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required for testing, but is not set", missingVar)
		return err
	}
	return nil
}

func main() {
	err := validateEnv()
	if err != nil {
		log.Fatalln(err)
	}

	kc, err := connutil.LoadKafkaConfig(model.AggregateID)
	if err != nil {
		err = errors.Wrap(err, "Error in KafkaConfig")
		log.Fatalln(err)
	}
	mc, err := connutil.LoadMongoConfig(model.AggregateID)
	if err != nil {
		err = errors.Wrap(err, "Error in MongoConfig")
		log.Fatalln(err)
	}
	ioConfig := poll.IOConfig{
		KafkaConfig: *kc,
		MongoConfig: *mc,
	}

	eventPoll, err := poll.Init(ioConfig)
	if err != nil {
		err = errors.Wrap(err, "Error creating EventPoll service")
		log.Fatalln(err)
	}

	eosToken := os.Getenv("KAFKA_END_OF_STREAM_TOKEN")
	for {
		select {
		case <-eventPoll.Context().Done():
			err = errors.New("service-context closed")
			log.Fatalln(err)

		case eventResp := <-eventPoll.Events():
			go func(eventResp *poll.EventResponse) {
				if eventResp == nil {
					return
				}
				err := eventResp.Error
				if err != nil {
					err = errors.Wrap(err, "Error in Query-EventResponse")
					log.Println(err)
					return
				}

				err = event.Handle(mc.AggCollection, eosToken, &eventResp.Event)
				if err != nil {
					err = errors.Wrap(err, "Error handling event")
					log.Println(err)
				}
			}(eventResp)
		}
	}
}
