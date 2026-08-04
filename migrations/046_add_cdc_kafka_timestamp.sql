-- Debezium EventRouter accepts a Connect Timestamp logical schema. The
-- authoritative recorded_at remains the canonical UTC timestamptz; this
-- technical UTC timestamp is emitted by Debezium's default adaptive mode as a
-- logical MicroTimestamp, which EventRouter converts to a Kafka timestamp.
ALTER TABLE event_store.events
  ADD COLUMN recorded_at_kafka timestamp without time zone;

ALTER TABLE event_store.events DISABLE TRIGGER events_reject_update_delete;
UPDATE event_store.events
  SET recorded_at_kafka = recorded_at AT TIME ZONE 'UTC';
ALTER TABLE event_store.events ENABLE TRIGGER events_reject_update_delete;

ALTER TABLE event_store.events
  ALTER COLUMN recorded_at_kafka SET NOT NULL;

CREATE FUNCTION event_store.fill_recorded_at_kafka()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE expected timestamp;
BEGIN
  expected := NEW.recorded_at AT TIME ZONE 'UTC';
  IF NEW.recorded_at_kafka IS NULL THEN
    NEW.recorded_at_kafka := expected;
  ELSIF NEW.recorded_at_kafka <> expected THEN
    RAISE EXCEPTION 'recorded_at_kafka must match recorded_at' USING ERRCODE = '22023';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER events_fill_recorded_at_kafka
BEFORE INSERT ON event_store.events
FOR EACH ROW EXECUTE FUNCTION event_store.fill_recorded_at_kafka();

GRANT SELECT(recorded_at_kafka) ON event_store.events TO event_store_cdc;
