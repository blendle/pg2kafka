CREATE OR REPLACE FUNCTION pg2kafka.enqueue_event() RETURNS trigger
LANGUAGE plpgsql
AS $_$
DECLARE
  external_id varchar;
  changes jsonb;
  col record;
  outbound_event record;
  notification json;
BEGIN
  SELECT pg2kafka.external_id_relations.external_id INTO external_id
  FROM pg2kafka.external_id_relations
  WHERE table_name = TG_TABLE_NAME;

  IF TG_OP = 'INSERT' THEN
    EXECUTE format('SELECT ($1).%s::text', external_id) USING NEW INTO external_id;
  ELSE
    EXECUTE format('SELECT ($1).%s::text', external_id) USING OLD INTO external_id;
  END IF;

  IF TG_OP = 'INSERT' THEN
    changes := row_to_json(NEW);
  ELSIF TG_OP = 'UPDATE' THEN
    changes := row_to_json(NEW);
    -- Remove object that didn't change
    FOR col IN SELECT * FROM jsonb_each(row_to_json(OLD)::jsonb) LOOP
      IF changes @> jsonb_build_object(col.key, col.value) THEN
        changes = changes - col.key;
      END IF;
    END LOOP;
  ELSIF TG_OP = 'DELETE' THEN
    changes := '{}'::jsonb;
  END IF;

  -- Don't enqueue an event for updates that did not change anything
  IF TG_OP = 'UPDATE' AND changes = '{}'::jsonb THEN
    RETURN NULL;
  END IF;

  INSERT INTO pg2kafka.outbound_event_queue(external_id, table_name, statement, data)
  VALUES (external_id, TG_TABLE_NAME, TG_OP, changes)
  RETURNING * INTO outbound_event;

  notification := row_to_json(outbound_event);
  PERFORM pg_notify('outbound_event_queue', notification::text);

  RETURN NULL;
END
$_$;

CREATE OR REPLACE FUNCTION pg2kafka.create_snapshot_events(table_name_ref regclass) RETURNS void
LANGUAGE plpgsql
AS $_$
DECLARE
  query text;
  rec record;
  changes jsonb;
  external_id_ref varchar;
  external_id varchar;
BEGIN
  SELECT pg2kafka.external_id_relations.external_id INTO external_id_ref
  FROM pg2kafka.external_id_relations
  WHERE pg2kafka.external_id_relations.table_name = table_name_ref::varchar;

  query := 'SELECT * FROM ' || table_name_ref;

  FOR rec IN EXECUTE query LOOP
    changes := row_to_json(rec);
    external_id := changes->>external_id_ref;

    INSERT INTO pg2kafka.outbound_event_queue(external_id, table_name, statement, data)
    VALUES (external_id, table_name_ref, 'SNAPSHOT', changes);
  END LOOP;

  PERFORM pg_notify('outbound_event_queue', '{ "message": "snapshot created" }');
END
$_$;

CREATE OR REPLACE FUNCTION pg2kafka.setup(table_name_ref regclass, external_id_name text) RETURNS void
LANGUAGE plpgsql
AS $_$
DECLARE
  existing_id varchar;
  trigger_name varchar;
  lock_query varchar;
  trigger_query varchar;
BEGIN
  SELECT pg2kafka.external_id_relations.external_id INTO existing_id
  FROM pg2kafka.external_id_relations
  WHERE pg2kafka.external_id_relations.table_name = table_name_ref::varchar;

  IF existing_id != '' THEN
    RETURN;
  END IF;

  INSERT INTO pg2kafka.external_id_relations(external_id, table_name)
  VALUES (external_id_name, table_name_ref);

  trigger_name := table_name_ref || '_enqueue_event';
  lock_query := 'LOCK TABLE ' || table_name_ref || ' IN ACCESS EXCLUSIVE MODE';
  trigger_query := 'CREATE TRIGGER ' || trigger_name
    || ' AFTER INSERT OR DElETE OR UPDATE ON ' || table_name_ref
    || ' FOR EACH ROW EXECUTE PROCEDURE pg2kafka.enqueue_event()';

  -- We aqcuire an exlusive lock on the table to ensure that we do not miss any
  -- events between snapshotting and once the trigger is added.
  EXECUTE lock_query;

  PERFORM pg2kafka.create_snapshot_events(table_name_ref);

  EXECUTE trigger_query;
END
$_$;
