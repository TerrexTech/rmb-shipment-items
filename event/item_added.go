package event

import (
	"encoding/json"

	cmodel "github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/TerrexTech/rmb-shipment-items/model"
	"github.com/pkg/errors"
)

func itemAdded(coll *mongo.Collection, event *cmodel.Event) error {
	item := &model.Item{}
	err := json.Unmarshal(event.Data, item)
	if err != nil {
		err = errors.Wrap(err, "Error unmarshalling event-data to item")
		return err
	}

	_, err = coll.InsertOne(item)
	if err != nil {
		err = errors.Wrap(err, "Error inserting item into database")
		return err
	}
	return nil
}
