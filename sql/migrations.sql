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

-- We aqcuire an exlusive lock on the table to ensure that we do not miss any
-- events between snapshotting and once the trigger is added.
-- TODO: Should we wrap this in a function, so you cannot (easily) add the
-- trigger without also snapshotting?
BEGIN;
  LOCK TABLE users IN ACCESS EXCLUSIVE MODE;

  SELECT create_snapshot_events('users');

  CREATE TRIGGER IF NOT EXISTS users_enqueue_events
  AFTER INSERT OR DELETE OR UPDATE ON users
  FOR EACH ROW EXECUTE PROCEDURE enqueue_events();
COMMIT;
