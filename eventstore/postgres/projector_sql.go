package postgres

import (
	"fmt"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/lib/pq"
)

const sqlFuncEventStreamNotify = `CREATE FUNCTION public.event_stream_notify ()
  RETURNS TRIGGER
LANGUAGE plpgsql AS $$
DECLARE
  channel text := TG_ARGV[0];
BEGIN
  PERFORM (
          WITH payload AS
          (
              SELECT NEW.no, NEW.event_name, NEW.metadata -> '_aggregate_id' AS aggregate_id
          )
          SELECT pg_notify(channel, row_to_json(payload)::text) FROM payload
          );
  RETURN NULL;
END;
$$;`

// sqlTriggerEventStreamNotify a helper to create the sql on a event store table
func sqlTriggerEventStreamNotifyTemplate(eventStreamName eventstore.StreamName, eventStreamTable string) string {
	triggerName := fmt.Sprintf("%s_notify", eventStreamTable)
	return fmt.Sprintf(
		`DO LANGUAGE plpgsql $$
		 BEGIN
		   IF NOT EXISTS(
		       SELECT * FROM information_schema.triggers
		       WHERE
		           event_object_schema = 'public' AND
		           event_object_table = %s AND
		           trigger_schema = 'public' AND
		           trigger_name = %s
		   )
		   THEN
		     CREATE TRIGGER %s
		       AFTER INSERT
		       ON %s
		       FOR EACH ROW
		     EXECUTE PROCEDURE event_stream_notify(%s);
		   END IF;
		 END;
		 $$`,
		quoteString(eventStreamTable),
		quoteString(triggerName),
		pq.QuoteIdentifier(triggerName),
		pq.QuoteIdentifier(eventStreamTable),
		quoteString(string(eventStreamName)),
	)
}

// StreamProjectorCreateSchema return the sql statement needed for the postgres database in order to use the StreamProjector
func StreamProjectorCreateSchema(projectionTable string, streamName eventstore.StreamName, streamTable string) []string {
	return []string{
		sqlFuncEventStreamNotify,
		sqlTriggerEventStreamNotifyTemplate(streamName, streamTable),

		fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s (
				no SERIAL,
				name VARCHAR(150) NOT NULL,
				position BIGINT NOT NULL DEFAULT 0,
				state JSONB NOT NULL DEFAULT ('{}'),
				PRIMARY KEY (no),
				UNIQUE (name)
			)`,
			pq.QuoteIdentifier(projectionTable),
		),
	}
}

// AggregateProjectorCreateSchema return the sql statement needed for the postgres database in order to use the AggregateProjector
func AggregateProjectorCreateSchema(projectionTable string, streamName eventstore.StreamName, streamTable string) []string {
	return []string{
		sqlFuncEventStreamNotify,
		sqlTriggerEventStreamNotifyTemplate(streamName, streamTable),

		fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s (
				no SERIAL,
				aggregate_id UUID UNIQUE NOT NULL,
				position BIGINT NOT NULL DEFAULT 0,
  				state JSONB,
  				PRIMARY KEY (no)
			)`,
			pq.QuoteIdentifier(projectionTable),
		),
	}
}
