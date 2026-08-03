CREATE SEQUENCE event_store.event_number_seq AS bigint MINVALUE 1 NO CYCLE;

CREATE TABLE event_store.streams (
  namespace text NOT NULL,
  aggregate_type text NOT NULL,
  aggregate_id uuid NOT NULL,
  current_revision bigint NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL,
  last_recorded_at timestamptz,
  PRIMARY KEY (namespace, aggregate_type, aggregate_id),
  CHECK (namespace ~ '^[a-z][a-z0-9-]{1,62}$'),
  CHECK (aggregate_type ~ '^[A-Z][A-Za-z0-9]{0,127}$'),
  CHECK (uuid_extract_version(aggregate_id) = 7),
  CHECK (current_revision >= 0)
) WITH (fillfactor = 80);

CREATE TABLE event_store.events (
  event_number bigint NOT NULL DEFAULT nextval('event_store.event_number_seq'),
  event_id uuid NOT NULL,
  namespace text NOT NULL,
  aggregate_type text NOT NULL,
  aggregate_id uuid NOT NULL,
  stream_revision bigint NOT NULL,
  request_id uuid NOT NULL,
  request_event_no smallint NOT NULL,
  event_name text NOT NULL,
  schema_version integer NOT NULL,
  occurred_at timestamptz NOT NULL,
  recorded_at timestamptz NOT NULL,
  producer_service text NOT NULL,
  topic_route text NOT NULL DEFAULT 'event-store',
  partition_key text NOT NULL,
  event_envelope jsonb NOT NULL,
  envelope_sha256 char(64) NOT NULL,
  PRIMARY KEY (event_number),
  UNIQUE (event_id),
  UNIQUE (namespace, aggregate_type, aggregate_id, stream_revision),
  UNIQUE (producer_service, request_id, request_event_no),
  FOREIGN KEY (namespace, aggregate_type, aggregate_id) REFERENCES event_store.streams ON UPDATE RESTRICT ON DELETE RESTRICT,
  CHECK (stream_revision > 0),
  CHECK (request_event_no BETWEEN 1 AND 100),
  CHECK (schema_version > 0),
  CHECK (topic_route = 'event-store'),
  CHECK (partition_key = namespace || '|' || aggregate_type || '|' || aggregate_id::text),
  CHECK (event_name ~ '^[a-z][a-z0-9]*(\.[a-z0-9]+)+$'),
  CHECK (producer_service ~ '^[a-z][a-z0-9-]{1,62}$'),
  CHECK (jsonb_typeof(event_envelope) = 'object'),
  CHECK (jsonb_typeof(event_envelope->'payload') = 'object'),
  CHECK (jsonb_typeof(event_envelope->'context') = 'object'),
  CHECK (event_envelope->>'eventId' = event_id::text),
  CHECK (event_envelope->>'namespace' = namespace),
  CHECK (event_envelope->>'aggregateType' = aggregate_type),
  CHECK (event_envelope->>'aggregateId' = aggregate_id::text),
  CHECK (event_envelope->>'streamRevision' = stream_revision::text),
  CHECK (event_envelope->>'eventNumber' = event_number::text),
  CHECK (event_envelope->>'eventName' = event_name),
  CHECK ((event_envelope->>'schemaVersion')::integer = schema_version),
  CHECK ((event_envelope->>'occurredAt')::timestamptz = occurred_at),
  CHECK ((event_envelope->>'recordedAt')::timestamptz = recorded_at),
  CHECK (event_envelope->>'producerService' = producer_service),
  CHECK (envelope_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX events_stream_read_idx ON event_store.events (namespace, aggregate_type, aggregate_id, stream_revision)
  INCLUDE (event_id, event_name, schema_version, occurred_at, recorded_at, event_envelope);
CREATE INDEX events_recorded_at_brin ON event_store.events USING brin (recorded_at) WITH (pages_per_range = 128);

CREATE TABLE event_store.append_requests (
  producer_service text NOT NULL,
  request_id uuid NOT NULL,
  request_sha256 bytea NOT NULL,
  namespace text NOT NULL,
  aggregate_type text NOT NULL,
  aggregate_id uuid NOT NULL,
  first_revision bigint NOT NULL,
  last_revision bigint NOT NULL,
  event_count smallint NOT NULL,
  response jsonb NOT NULL,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (producer_service, request_id),
  CHECK (uuid_extract_version(request_id) = 7),
  CHECK (octet_length(request_sha256) = 32),
  CHECK (event_count BETWEEN 1 AND 100),
  CHECK (first_revision > 0),
  CHECK (last_revision = first_revision + event_count - 1)
);

CREATE TABLE event_store.snapshots (
  namespace text NOT NULL,
  aggregate_type text NOT NULL,
  aggregate_id uuid NOT NULL,
  snapshot_revision bigint NOT NULL,
  reducer_version text NOT NULL,
  state_schema_version integer NOT NULL,
  state jsonb NOT NULL,
  state_sha256 bytea NOT NULL,
  created_at timestamptz NOT NULL,
  PRIMARY KEY (namespace, aggregate_type, aggregate_id, snapshot_revision, reducer_version),
  FOREIGN KEY (namespace, aggregate_type, aggregate_id, snapshot_revision)
    REFERENCES event_store.events (namespace, aggregate_type, aggregate_id, stream_revision) ON UPDATE RESTRICT ON DELETE RESTRICT,
  CHECK (snapshot_revision > 0), CHECK (snapshot_revision % 1000 = 0), CHECK (state_schema_version > 0),
  CHECK (jsonb_typeof(state) = 'object'), CHECK (octet_length(state_sha256) = 32),
  CHECK (reducer_version ~ '^[a-f0-9]{64}$')
);
CREATE INDEX snapshots_latest_idx ON event_store.snapshots (namespace, aggregate_type, aggregate_id, reducer_version, snapshot_revision DESC)
  INCLUDE (state_schema_version, state, state_sha256, created_at);

CREATE FUNCTION event_store.reject_event_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'event_store.events is immutable' USING ERRCODE = '55000'; END $$;
CREATE TRIGGER events_reject_update_delete BEFORE UPDATE OR DELETE ON event_store.events FOR EACH ROW EXECUTE FUNCTION event_store.reject_event_mutation();
CREATE TRIGGER append_requests_reject_update_delete BEFORE UPDATE OR DELETE ON event_store.append_requests FOR EACH ROW EXECUTE FUNCTION event_store.reject_event_mutation();
