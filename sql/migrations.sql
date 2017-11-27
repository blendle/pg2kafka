CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE SCHEMA IF NOT EXISTS pg2kafka;

CREATE SEQUENCE IF NOT EXISTS pg2kafka.outbound_event_queue_id;
CREATE TABLE IF NOT EXISTS pg2kafka.outbound_event_queue (
  id            integer NOT NULL DEFAULT nextval('pg2kafka.outbound_event_queue_id'::regclass),
  uuid          uuid NOT NULL DEFAULT uuid_generate_v4(),
  external_id   varchar(255),
  table_name    varchar(255) NOT NULL,
  statement     varchar(20) NOT NULL,
  data          jsonb NOT NULL,
  created_at    timestamp NOT NULL DEFAULT current_timestamp,
  processed     boolean DEFAULT false
);

CREATE INDEX IF NOT EXISTS outbound_event_queue_id_index
ON pg2kafka.outbound_event_queue (id);

CREATE SEQUENCE IF NOT EXISTS pg2kafka.external_id_relations_id;
CREATE TABLE IF NOT EXISTS pg2kafka.external_id_relations (
  id            integer NOT NULL DEFAULT nextval('pg2kafka.external_id_relations_id'::regclass),
  external_id   varchar(255) NOT NULL,
  table_name    varchar(255) NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS external_id_relations_unique_table_name_index
ON pg2kafka.external_id_relations(table_name);
