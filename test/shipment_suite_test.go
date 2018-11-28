package test

import (
	"encoding/json"
	"log"
	"os"
	"testing"
	"time"

	"github.com/TerrexTech/rmb-shipment-items/connutil"

	"github.com/Shopify/sarama"
	cmodel "github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-kafkautils/kafka"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/rmb-shipment-items/model"
	"github.com/TerrexTech/uuuid"

	"github.com/TerrexTech/go-commonutils/commonutil"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// TestEvent tests Event-handling.
func TestEvent(t *testing.T) {
	log.Println("Reading environment file")
	err := godotenv.Load("../.env")
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
		"KAFKA_PRODUCER_EVENT_TOPIC",

		"KAFKA_END_OF_STREAM_TOKEN",

		"MONGO_HOSTS",
		"MONGO_DATABASE",
		"MONGO_AGG_COLLECTION",
		"MONGO_META_COLLECTION",

		"MONGO_CONNECTION_TIMEOUT_MS",
	)

	if err != nil {
		err = errors.Wrapf(err, "Env-var %s is required for testing, but is not set", missingVar)
		log.Fatalln(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "EventHandler Suite")
}

func mockEvent(
	input chan<- *sarama.ProducerMessage,
	action string,
	data []byte,
	topic string,
) *cmodel.Event {
	eventUUID, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())
	cid, err := uuuid.NewV4()
	Expect(err).ToNot(HaveOccurred())

	mockEvent := &cmodel.Event{
		AggregateID:   model.AggregateID,
		Action:        action,
		CorrelationID: cid,
		Data:          data,
		Source:        "test-source",
		NanoTime:      time.Now().UnixNano(),
		UUID:          eventUUID,
		YearBucket:    2018,
	}

	// Produce command on Kafka topic
	testEventMsg, err := json.Marshal(mockEvent)
	Expect(err).ToNot(HaveOccurred())

	input <- kafka.CreateMessage(topic, testEventMsg)
	log.Printf("====> Produced mock event: %s on topic: %s", eventUUID, topic)
	return mockEvent
}

var _ = Describe("ItemsReadModel", func() {
	var (
		coll *mongo.Collection

		eventTopic string
		producer   *kafka.Producer
	)

	BeforeSuite(func() {
		eventTopic = os.Getenv("KAFKA_PRODUCER_EVENT_TOPIC")

		kafkaBrokersStr := os.Getenv("KAFKA_BROKERS")
		kafkaBrokers := *commonutil.ParseHosts(kafkaBrokersStr)

		var err error
		producer, err = kafka.NewProducer(&kafka.ProducerConfig{
			KafkaBrokers: kafkaBrokers,
		})
		Expect(err).ToNot(HaveOccurred())

		mc, err := connutil.LoadMongoConfig(model.AggregateID)
		Expect(err).ToNot(HaveOccurred())
		coll = mc.AggCollection
	})

	Describe("ItemAdded-Event", func() {
		It("should insert item into database", func() {
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			custID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockItem := model.Item{
				ItemID:       itemID.String(),
				DateArrived:  time.Now().UTC().Unix(),
				Lot:          "test-lot",
				Name:         "test-name",
				Origin:       "test-origin",
				Price:        12.3,
				RSCustomerID: custID.String(),
				SKU:          "test-sku",
				Timestamp:    time.Now().UTC().Unix(),
				TotalWeight:  4.7,
				UPC:          "test-upc",
			}
			marshalItem, err := json.Marshal(mockItem)
			Expect(err).ToNot(HaveOccurred())

			mockEvent(producer.Input(), "ItemAdded", marshalItem, eventTopic)
			time.Sleep(5 * time.Second)

			_, err = coll.FindOne(mockItem)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Describe("ItemUpdated-Event", func() {
		It("should update item in database", func() {
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			custID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockItem := model.Item{
				ItemID:       itemID.String(),
				DateArrived:  time.Now().UTC().Unix(),
				Lot:          "test-lot",
				Name:         "test-name",
				Origin:       "test-origin",
				Price:        12.3,
				RSCustomerID: custID.String(),
				SKU:          "test-sku",
				Timestamp:    time.Now().UTC().Unix(),
				TotalWeight:  4.7,
				UPC:          "test-upc",
			}
			_, err = coll.InsertOne(mockItem)
			Expect(err).ToNot(HaveOccurred())

			newLot, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			updateParams := map[string]interface{}{
				"filter": &model.Item{
					ItemID: itemID.String(),
				},
				"update": &model.Item{
					Lot: newLot.String(),
				},
			}
			marshalParams, err := json.Marshal(updateParams)
			Expect(err).ToNot(HaveOccurred())

			mockEvent(producer.Input(), "ItemUpdated", marshalParams, eventTopic)
			time.Sleep(5 * time.Second)

			mockItem.Lot = newLot.String()
			_, err = coll.FindOne(mockItem)
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Describe("ItemDeleted-Event", func() {
		It("should delete item from database", func() {
			itemID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			custID, err := uuuid.NewV4()
			Expect(err).ToNot(HaveOccurred())
			mockItem := model.Item{
				ItemID:       itemID.String(),
				DateArrived:  time.Now().UTC().Unix(),
				Lot:          "test-lot",
				Name:         "test-name",
				Origin:       "test-origin",
				Price:        12.3,
				RSCustomerID: custID.String(),
				SKU:          "test-sku",
				Timestamp:    time.Now().UTC().Unix(),
				TotalWeight:  4.7,
				UPC:          "test-upc",
			}
			_, err = coll.InsertOne(mockItem)
			Expect(err).ToNot(HaveOccurred())

			deleteParams := map[string]interface{}{
				"itemID": itemID.String(),
			}
			marshalParams, err := json.Marshal(deleteParams)
			Expect(err).ToNot(HaveOccurred())

			mockEvent(producer.Input(), "ItemDeleted", marshalParams, eventTopic)
			time.Sleep(5 * time.Second)

			_, err = coll.FindOne(mockItem)
			Expect(err).To(HaveOccurred())
		})
	})
})
