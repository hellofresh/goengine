// +build unit

package sql_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/aggregate"
	"github.com/hellofresh/goengine/metadata"
	"github.com/hellofresh/goengine/mocks"
	"github.com/hellofresh/goengine/strategy/json/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nameChanged struct {
	name string
}

func TestAggregateChangedFactory_CreateFromRows(t *testing.T) {
	rowColumns := []string{"no", "event_id", "event_name", "payload", "metadata", "created_at"}

	t.Run("reconstruct messages", func(t *testing.T) {
		type validTestCase struct {
			title            string
			expectedMessages func(t *testing.T) []*aggregate.Changed
		}

		testCases := []validTestCase{
			{
				"no rows",
				func(t *testing.T) []*aggregate.Changed {
					return []*aggregate.Changed{}
				},
			},
			{
				"create aggregate.Changed messages from rows",
				func(t *testing.T) []*aggregate.Changed {
					asserts := assert.New(t)

					// Create expectations
					expectedMessage1, err := createAggregateChangedMessage(nameChanged{"bob"}, 1)
					if !asserts.Nil(err) {
						t.FailNow()
						return nil
					}

					expectedMessage2, err := createAggregateChangedMessage(nameChanged{"alice"}, 2)
					if !asserts.Nil(err) {
						t.FailNow()
						return nil
					}

					return []*aggregate.Changed{
						expectedMessage1,
						expectedMessage2,
					}
				},
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				expectedMessages := testCase.expectedMessages(t)

				asserts := assert.New(t)

				// Mock payload factory and rows
				var expectedMessageNumbers []int64
				payloadFactory := &mocks.PayloadFactory{}
				mockRows := sqlmock.NewRows(rowColumns)
				for i, msg := range expectedMessages {
					rowPayload, err := json.Marshal(msg.Payload())
					if !asserts.Nil(err) {
						return
					}

					rowMetadata, err := json.Marshal(msg.Metadata())
					if !asserts.Nil(err) {
						return
					}

					msgNr := i + 1
					uuid, _ := msg.UUID().MarshalBinary()
					payloadFactory.On("CreatePayload", "name_changed", rowPayload).Once().Return(msg.Payload(), nil)
					mockRows.AddRow(msgNr, uuid, "name_changed", rowPayload, rowMetadata, msg.CreatedAt())
					expectedMessageNumbers = append(expectedMessageNumbers, int64(msgNr))
				}

				// A little overhead but we need to query in order to get sql.Rows
				db, dbMock, err := sqlmock.New()
				if !asserts.Nil(err) {
					return
				}
				defer db.Close()

				dbMock.ExpectQuery("SELECT").WillReturnRows(mockRows)
				rows, err := db.Query("SELECT")
				if !asserts.Nil(err) {
					return
				}
				defer rows.Close()

				// Create the factory
				messageFactory, err := sql.NewAggregateChangedFactory(payloadFactory)
				if !asserts.Nil(err) {
					return
				}

				// Finally recreate the messages
				stream, err := messageFactory.CreateEventStream(rows)
				if !asserts.NoError(err) {
					return
				}
				defer stream.Close()

				messages, messageNumbers, err := goengine.ReadEventStream(stream)
				require.NoError(t, err)
				require.NoError(t, stream.Err(), "no exception was expected while reading the stream")

				assertEqualMessages(t, expectedMessages, messages)
				asserts.Equal(expectedMessageNumbers, messageNumbers)
				payloadFactory.AssertExpectations(t)
			})
		}
	})

	t.Run("no rows", func(t *testing.T) {
		asserts := assert.New(t)

		// Create the factory
		messageFactory, err := sql.NewAggregateChangedFactory(&mocks.PayloadFactory{})
		if !asserts.Nil(err) {
			return
		}

		// Finally recreate the messages
		msgs, err := messageFactory.CreateEventStream(nil)

		// Check result
		if asserts.Error(err) {
			arg := err.(goengine.InvalidArgumentError)
			asserts.Equal("rows", string(arg))
		}
		asserts.Nil(msgs)
	})

	t.Run("invalid row", func(t *testing.T) {
		type invalidTestCase struct {
			title                string
			mockRows             func(t *testing.T) (*sqlmock.Rows, *mocks.PayloadFactory)
			expectedErrorMessage string
		}

		testCases := []invalidTestCase{
			{
				"invalid columns in row",
				func(t *testing.T) (*sqlmock.Rows, *mocks.PayloadFactory) {
					mockRows := sqlmock.NewRows([]string{"invalid"})
					mockRows.AddRow("test")

					return mockRows, &mocks.PayloadFactory{}
				},
				"sql: expected 1 destination arguments in Scan, not 6",
			},
			{
				"bad metadata json",
				func(t *testing.T) (*sqlmock.Rows, *mocks.PayloadFactory) {
					uuid, _ := goengine.GenerateUUID().MarshalBinary()
					mockRows := sqlmock.NewRows(rowColumns)
					mockRows.AddRow(1, uuid, "some", []byte("{}"), []byte(`[ "missing array end" `), time.Now().UTC())

					return mockRows, &mocks.PayloadFactory{}
				},
				"unexpected end of JSON input",
			},
			{
				"bad payload",
				func(t *testing.T) (*sqlmock.Rows, *mocks.PayloadFactory) {
					uuid, _ := goengine.GenerateUUID().MarshalBinary()
					mockRows := sqlmock.NewRows(rowColumns)
					mockRows.AddRow(
						1,
						uuid,
						"some",
						[]byte("{}"),
						[]byte("{}"),
						time.Now().UTC(),
					)

					factory := &mocks.PayloadFactory{}
					factory.On("CreatePayload", "some", []byte("{}")).Once().Return(nil, errors.New("bad payload"))

					return mockRows, factory
				},
				"bad payload",
			},
			{
				"missing aggregate id",
				func(t *testing.T) (*sqlmock.Rows, *mocks.PayloadFactory) {
					uuid, _ := goengine.GenerateUUID().MarshalBinary()
					mockRows := sqlmock.NewRows(rowColumns)
					mockRows.AddRow(
						1,
						uuid,
						"some",
						[]byte("{}"),
						[]byte("{}"),
						time.Now().UTC(),
					)

					factory := &mocks.PayloadFactory{}
					factory.On("CreatePayload", "some", []byte("{}")).Once().Return(struct{}{}, nil)

					return mockRows, factory
				},
				"goengine: metadata key _aggregate_id is not set or nil",
			},
			{
				"missing aggregate version",
				func(t *testing.T) (*sqlmock.Rows, *mocks.PayloadFactory) {
					uuid, _ := goengine.GenerateUUID().MarshalBinary()
					mockRows := sqlmock.NewRows(rowColumns)
					mockRows.AddRow(
						1,
						uuid,
						"some",
						[]byte("{}"),
						[]byte(`{"_aggregate_id": "00c5ca66-df07-4fcc-8866-5ca6ba1a10b8"}`),
						time.Now().UTC(),
					)

					factory := &mocks.PayloadFactory{}
					factory.On("CreatePayload", "some", []byte("{}")).Once().Return(struct{}{}, nil)

					return mockRows, factory
				},
				"goengine: metadata key _aggregate_version is not set or nil",
			},
			{
				"invalid aggregate version type",
				func(t *testing.T) (*sqlmock.Rows, *mocks.PayloadFactory) {
					uuid, _ := goengine.GenerateUUID().MarshalBinary()
					mockRows := sqlmock.NewRows(rowColumns)
					mockRows.AddRow(
						1,
						uuid,
						"some",
						[]byte("{}"),
						[]byte(`{
							"_aggregate_id": "00c5ca66-df07-4fcc-8866-5ca6ba1a10b8",
							"_aggregate_version": "string"
						}`),
						time.Now().UTC(),
					)

					factory := &mocks.PayloadFactory{}
					factory.On("CreatePayload", "some", []byte("{}")).Once().Return(struct{}{}, nil)

					return mockRows, factory
				},
				"goengine: metadata key _aggregate_version with value string was expected to be of type float64",
			},
			{
				"invalid aggregate version",
				func(t *testing.T) (*sqlmock.Rows, *mocks.PayloadFactory) {
					uuid, _ := goengine.GenerateUUID().MarshalBinary()
					mockRows := sqlmock.NewRows(rowColumns)
					mockRows.AddRow(
						1,
						uuid,
						"some",
						[]byte("{}"),
						[]byte(`{
							"_aggregate_id": "00c5ca66-df07-4fcc-8866-5ca6ba1a10b8",
							"_aggregate_version": -1
						}`),
						time.Now().UTC(),
					)

					factory := &mocks.PayloadFactory{}
					factory.On("CreatePayload", "some", []byte("{}")).Once().Return(struct{}{}, nil)

					return mockRows, factory
				},
				"goengine: a changed event must have a version number greater than zero",
			},
		}

		for _, testCase := range testCases {
			t.Run(testCase.title, func(t *testing.T) {
				asserts := assert.New(t)

				mockRows, payloadFactory := testCase.mockRows(t)

				// A little overhead but we need to query in order to get sql.Rows
				db, dbMock, err := sqlmock.New()
				if !asserts.Nil(err) {
					return
				}
				defer db.Close()

				dbMock.ExpectQuery("SELECT").WillReturnRows(mockRows)
				rows, err := db.Query("SELECT")
				if !asserts.Nil(err) {
					return
				}
				defer rows.Close()

				// Create the factory
				messageFactory, err := sql.NewAggregateChangedFactory(payloadFactory)
				if !asserts.Nil(err) {
					return
				}

				// Finally recreate the messages
				stream, err := messageFactory.CreateEventStream(rows)
				if !asserts.NoError(err) {
					asserts.FailNow("no exception was expected")
				}
				defer stream.Close()

				// Read the stream
				messages, _, err := goengine.ReadEventStream(stream)
				if !asserts.NoError(stream.Err()) {
					asserts.FailNow("no exception was expected while reading the stream")
				}

				asserts.EqualError(err, testCase.expectedErrorMessage)
				asserts.Nil(messages)

				payloadFactory.AssertExpectations(t)
			})
		}
	})
}

func createAggregateChangedMessage(payload interface{}, version uint) (*aggregate.Changed, error) {
	id := aggregate.GenerateID()
	msg, err := aggregate.ReconstituteChange(
		id,
		goengine.GenerateUUID(),
		payload,
		metadata.WithValue(
			metadata.WithValue(
				metadata.WithValue(metadata.New(), aggregate.TypeKey, "person"),
				aggregate.VersionKey,
				float64(version),
			),
			aggregate.IDKey,
			string(id),
		),
		time.Now().UTC(),
		version,
	)

	return msg, err
}

func assertEqualMessages(t *testing.T, expected []*aggregate.Changed, msgs []goengine.Message) {
	asserts := assert.New(t)
	if asserts.Len(msgs, len(expected)) {
		return
	}

	for i, msg := range msgs {
		expectedMsg := expected[i]

		asserts.Equalf(expectedMsg.UUID(), msg.UUID(), "Message %d: UUID should match", i)
		asserts.Equalf(expectedMsg.Payload(), msg.Payload(), "Message %d: Payload should match", i)
		asserts.Equalf(expectedMsg.CreatedAt(), msg.CreatedAt(), "Message %d: Payload should match", i)
		asserts.Equalf(expectedMsg.Metadata().AsMap(), msg.Metadata().AsMap(), "Message %d: Payload should match", i)

		if asserts.IsType((*aggregate.Changed)(nil), msg, "Message %d: Should be of type aggregate.Changed") {
			changedMsg := msg.(*aggregate.Changed)
			asserts.Equalf(expectedMsg.AggregateID(), changedMsg.AggregateID(), "Message %d: AggregateID should match", i)
			asserts.Equalf(expectedMsg.Version(), changedMsg.Version(), "Message %d: Version should match", i)
		}
	}
}
