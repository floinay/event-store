-- A btree INCLUDE value is stored in the index tuple and is limited to one
-- eighth of an 8 KiB page. Keeping event_envelope there rejected valid 1 MiB
-- appends before PostgreSQL could TOAST the JSONB value.
DROP INDEX event_store.events_stream_read_idx;

CREATE INDEX events_stream_read_idx
  ON event_store.events (namespace, aggregate_type, aggregate_id, stream_revision)
  INCLUDE (event_id, event_name, schema_version, occurred_at, recorded_at);
