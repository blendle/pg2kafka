CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA IF NOT EXISTS pg2kafka;

CREATE SEQUENCE IF NOT EXISTS pg2kafka.outbound_event_queue_id;
CREATE TABLE IF NOT EXISTS pg2kafka.outbound_event_queue (
  id            integer NOT NULL DEFAULT nextval('pg2kafka.outbound_event_queue_id'::regclass),
  uuid          uuid NOT NULL DEFAULT uuid_generate_v4(),
  external_id   varchar(100) NOT NULL,
  table_name    varchar(100) NOT NULL,
  statement     varchar(20) NOT NULL,
  data          jsonb NOT NULL,
  created_at    timestamp NOT NULL DEFAULT current_timestamp,
  processed     boolean DEFAULT false
);

CREATE INDEX IF NOT EXISTS outbound_event_queue_id_not_processed_index
ON pg2kafka.outbound_event_queue (id)
WHERE processed IS FALSE;
