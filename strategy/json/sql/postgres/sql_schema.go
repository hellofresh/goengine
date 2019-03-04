package postgres

import (
	"fmt"

	"github.com/hellofresh/goengine"
	"github.com/hellofresh/goengine/driver/sql/postgres"
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
func sqlTriggerEventStreamNotifyTemplate(eventStreamName goengine.StreamName, eventStreamTable string) string {
	triggerName := fmt.Sprintf("%s_notify", eventStreamTable)
	/* #nosec G201 */
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
		postgres.QuoteString(eventStreamTable),
		postgres.QuoteString(triggerName),
		postgres.QuoteIdentifier(triggerName),
		postgres.QuoteIdentifier(eventStreamTable),
		postgres.QuoteString(string(eventStreamName)),
	)
}

// StreamProjectorCreateSchema return the sql statement needed for the postgres database in order to use the StreamProjector
func StreamProjectorCreateSchema(projectionTable string, streamName goengine.StreamName, streamTable string) []string {
	/* #nosec G201 */
	return []string{
		sqlFuncEventStreamNotify,
		sqlTriggerEventStreamNotifyTemplate(streamName, streamTable),

		fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s (
				no SERIAL,
				name VARCHAR(150) UNIQUE NOT NULL,
				position BIGINT NOT NULL DEFAULT 0,
				state JSONB NOT NULL DEFAULT ('{}'),
				locked BOOLEAN NOT NULL DEFAULT (FALSE), 
				PRIMARY KEY (no)
			)`,
			postgres.QuoteIdentifier(projectionTable),
		),
	}
}

// AggregateProjectorCreateSchema return the sql statement needed for the postgres database in order to use the AggregateProjector
func AggregateProjectorCreateSchema(projectionTable string, streamName goengine.StreamName, streamTable string) []string {
	/* #nosec G201 */
	return []string{
		sqlFuncEventStreamNotify,
		sqlTriggerEventStreamNotifyTemplate(streamName, streamTable),

		fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s (
				no SERIAL,
				aggregate_id UUID UNIQUE NOT NULL,
				position BIGINT NOT NULL DEFAULT 0,
  				state JSONB,
				locked BOOLEAN NOT NULL DEFAULT (FALSE),
				failed BOOLEAN NOT NULL DEFAULT (FALSE),
  				PRIMARY KEY (no)
			)`,
			postgres.QuoteIdentifier(projectionTable),
		),
	}
}
