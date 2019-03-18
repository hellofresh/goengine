package postgres

import (
	"errors"
)

// defaultProjectionStateEncoder this `ProjectionStateEncoder` is used for a goeninge.Projection
func defaultProjectionStateEncoder(state interface{}) ([]byte, error) {
	if state == nil {
		return []byte{'{', '}'}, nil
	}

	return nil, errors.New("unexpected state provided (Did you forget to implement goengine.ProjectionSaga?)")
}
