CREATE OR REPLACE PROCEDURAL LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION enqueue_event() RETURNS trigger
LANGUAGE plpgsql
AS $_$
DECLARE
  external_id varchar;
  changes jsonb;
  col record;
  outbound_event record;
  notification json;
BEGIN
  IF TG_OP = 'INSERT' THEN
    external_id := NEW.uuid; -- TODO: uuid or uid or id?
    changes := row_to_json(NEW);
  ELSIF TG_OP = 'UPDATE' THEN
    external_id := OLD.uuid;
    changes := row_to_json(NEW);
    -- Remove object that didn't change
    FOR col IN SELECT * FROM jsonb_each(row_to_json(OLD)::jsonb) LOOP
      IF changes @> jsonb_build_object(col.key, col.value) THEN
        changes = changes - col.key;
      END IF;
    END LOOP;
  ELSIF TG_OP = 'DELETE' THEN
    external_id := OLD.uuid;
    changes := '{}'::jsonb;
  END IF;

  -- Don't enqueue an event for updates that did not change anything
  IF TG_OP = 'UPDATE' AND changes = '{}'::jsonb THEN
    RETURN NULL;
  END IF;

  INSERT INTO outbound_event_queue(external_id, table_name, statement, data)
  VALUES (external_id, TG_TABLE_NAME, TG_OP, changes)
  RETURNING * INTO outbound_event;

  notification := row_to_json(outbound_event);
  PERFORM pg_notify('outbound_event_queue', notification::text);

  RETURN NULL;
END
$_$;

CREATE OR REPLACE FUNCTION create_snapshot_events(table_name regclass) RETURNS void
LANGUAGE plpgsql
AS $_$
DECLARE
  query text;
  rec record;
  changes jsonb;
  external_id varchar;
BEGIN
  query := 'SELECT * FROM ' || table_name;

  FOR rec IN EXECUTE query LOOP
    external_id := rec.uuid; -- TODO: uuid / uid / id
    changes := json_strip_nulls(row_to_json(rec));

    INSERT INTO outbound_event_queue(external_id, table_name, statement, data)
    VALUES (external_id, table_name, 'SNAPSHOT', changes);
  END LOOP;
END
$_$;

CREATE OR REPLACE FUNCTION setup_pg2kafka(table_name regclass) RETURNS void
LANGUAGE plpgsql
AS $_$
DECLARE
  trigger_name varchar;
  lock_query varchar;
  trigger_query varchar;
BEGIN
  trigger_name := table_name || '_enqueue_event';
  lock_query := 'LOCK TABLE ' || table_name || ' IN ACCESS EXCLUSIVE MODE';
  trigger_query := 'CREATE TRIGGER ' || trigger_name
    || ' AFTER INSERT OR DElETE OR UPDATE ON ' || table_name
    || ' FOR EACH ROW EXECUTE PROCEDURE enqueue_event()';

  -- We aqcuire an exlusive lock on the table to ensure that we do not miss any
  -- events between snapshotting and once the trigger is added.
  EXECUTE lock_query;

  PERFORM create_snapshot_events(table_name);

  EXECUTE trigger_query;
END
$_$;
