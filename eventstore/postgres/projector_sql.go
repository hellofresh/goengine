package postgres

import (
	"fmt"

	"github.com/hellofresh/goengine/eventstore"
	"github.com/lib/pq"
)

// StreamProjectorCreateSchema return the sql statement needed for the postgres database in order to use the StreamProjector
func StreamProjectorCreateSchema(projectionTable string, streamName eventstore.StreamName, streamTable string) []string {
	statements := make([]string, 3)
	statements[0] = `CREATE FUNCTION public.event_stream_notify ()
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

	statements[1] = fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (
			no SERIAL,
			name VARCHAR(150) NOT NULL,
			position BIGINT NOT NULL DEFAULT 0,
			state JSONB NOT NULL DEFAULT ('{}'),
			PRIMARY KEY (no),
			UNIQUE (name)
		)`,
		pq.QuoteIdentifier(projectionTable),
	)

	triggerName := fmt.Sprintf("%s_notify", streamTable)
	statements[2] = fmt.Sprintf(
		`DO LANGUAGE plpgsql $$
		 BEGIN
		   IF NOT EXISTS(
		       SELECT * FROM information_schema.triggers
		       WHERE
		           event_object_table = %s AND
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
		quoteString(streamTable),
		quoteString(triggerName),
		pq.QuoteIdentifier(triggerName),
		pq.QuoteIdentifier(streamTable),
		quoteString(string(streamName)),
	)

	return statements
}
