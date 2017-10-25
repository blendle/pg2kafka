CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SEQUENCE IF NOT EXISTS outbound_event_queue_id;
CREATE TABLE IF NOT EXISTS outbound_event_queue (
  id            integer NOT NULL DEFAULT nextval('outbound_event_queue_id'::regclass),
  uuid          uuid NOT NULL DEFAULT uuid_generate_v4(),
  external_id   varchar(100) NOT NULL,
  table_name    varchar(100) NOT NULL,
  statement     varchar(20) NOT NULL,
  data          jsonb NOT NULL,
  created_at    timestamp NOT NULL DEFAULT current_timestamp,
  processed     boolean DEFAULT false
);

CREATE INDEX outbound_event_queue_id_not_processed_index
ON outbound_event_queue (id)
WHERE processed IS FALSE;
