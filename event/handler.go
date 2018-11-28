package event

import (
	"fmt"

	"github.com/TerrexTech/go-common-models/model"
	"github.com/TerrexTech/go-mongoutils/mongo"
	"github.com/pkg/errors"
)

// Handle handles the provided event.
func Handle(coll *mongo.Collection, eosToken string, event *model.Event) error {
	if coll == nil {
		return errors.New("coll cannot be nil")
	}
	if event == nil || event.Action == eosToken {
		return nil
	}

	switch event.Action {
	case "ItemAdded":
		err := itemAdded(coll, event)
		if err != nil {
			err = errors.Wrap(err, "Error processing ItemAdded-event")
			return err
		}
		return nil

	case "ItemDeleted":
		err := itemDeleted(coll, event)
		if err != nil {
			err = errors.Wrap(err, "Error processing ItemDeleted-event")
			return err
		}
		return nil

	case "ItemUpdated":
		err := itemUpdated(coll, event)
		if err != nil {
			err = errors.Wrap(err, "Error processing ItemUpdated-event")
			return err
		}
		return nil

	default:
		return fmt.Errorf("unregistered Action: %s", event.Action)
	}
}
