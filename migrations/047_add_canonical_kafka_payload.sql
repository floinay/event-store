-- jsonb preserves the envelope value but not its canonical byte representation.
-- This derived column is the immutable payload source for Debezium EventRouter.
ALTER TABLE event_store.events
  ADD COLUMN event_envelope_kafka text;

ALTER TABLE event_store.events DISABLE TRIGGER events_reject_update_delete;
UPDATE event_store.events
  SET event_envelope_kafka = event_store.canonical_jsonb(event_envelope);
ALTER TABLE event_store.events ENABLE TRIGGER events_reject_update_delete;

ALTER TABLE event_store.events
  ALTER COLUMN event_envelope_kafka SET NOT NULL;

CREATE FUNCTION event_store.fill_event_envelope_kafka()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE expected text;
BEGIN
  expected := event_store.canonical_jsonb(NEW.event_envelope);
  IF NEW.event_envelope_kafka IS NULL THEN
    NEW.event_envelope_kafka := expected;
  ELSIF NEW.event_envelope_kafka <> expected THEN
    RAISE EXCEPTION 'event_envelope_kafka must be canonical event_envelope'
      USING ERRCODE = '22023';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER events_fill_event_envelope_kafka
BEFORE INSERT ON event_store.events
FOR EACH ROW EXECUTE FUNCTION event_store.fill_event_envelope_kafka();
