CREATE OR REPLACE FUNCTION event_store.canonical_jsonb(p_value jsonb)
RETURNS text
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
SET search_path = pg_catalog
AS $$
  SELECT CASE jsonb_typeof($1)
    WHEN 'object' THEN COALESCE((
      SELECT '{' || string_agg(to_json(key)::text || ':' || event_store.canonical_jsonb($1 -> key), ',' ORDER BY key COLLATE "C") || '}'
      FROM jsonb_object_keys($1) AS keys(key)
    ), '{}')
    WHEN 'array' THEN COALESCE((
      SELECT '[' || string_agg(event_store.canonical_jsonb(value), ',' ORDER BY ordinal) || ']'
      FROM jsonb_array_elements($1) WITH ORDINALITY AS entries(value, ordinal)
    ), '[]')
    ELSE $1::text
  END
$$;

CREATE OR REPLACE FUNCTION event_store.append_v1(
  p_producer_service text, p_namespace text, p_aggregate_type text, p_aggregate_id uuid,
  p_request_id uuid, p_expected_kind text, p_expected_revision bigint, p_events jsonb, p_context jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
DECLARE
  v_request_hash bytea; v_existing_hash bytea; v_existing_result jsonb; v_current bigint;
  v_created integer; v_first bigint; v_last bigint; v_recorded_at timestamptz; v_recorded_text text;
  v_response_events jsonb := '[]'::jsonb; v_response jsonb; v_draft jsonb; v_ord integer;
  v_event_id uuid; v_event_number bigint; v_revision bigint; v_occurred_at timestamptz;
  v_envelope jsonb; v_envelope_hash text;
BEGIN
  IF p_expected_kind NOT IN ('no_stream', 'exact') THEN RAISE EXCEPTION 'invalid expected revision kind' USING ERRCODE = '22023'; END IF;
  IF p_expected_kind = 'exact' AND (p_expected_revision IS NULL OR p_expected_revision < 0) THEN RAISE EXCEPTION 'expected revision is required' USING ERRCODE = '22023'; END IF;
  IF jsonb_typeof(p_events) <> 'array' OR jsonb_array_length(p_events) NOT BETWEEN 1 AND 100 THEN RAISE EXCEPTION 'events count must be 1..100' USING ERRCODE = '22023'; END IF;
  IF jsonb_typeof(p_context) <> 'object' OR p_context->>'correlationId' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    OR (p_context ? 'causationId' AND p_context->>'causationId' IS NOT NULL AND p_context->>'causationId' !~ '^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$')
    OR p_context->'actor'->>'kind' NOT IN ('user', 'service', 'system') OR coalesce(p_context->'actor'->>'subjectRef', '') = ''
  THEN RAISE EXCEPTION 'invalid event context' USING ERRCODE = '22023'; END IF;
  IF octet_length(p_events::text) + octet_length(p_context::text) > 1048576 THEN RAISE EXCEPTION 'append payload exceeds 1 MiB' USING ERRCODE = '22001'; END IF;

  v_request_hash := digest(jsonb_build_object(
    'producerService', p_producer_service, 'namespace', p_namespace, 'aggregateType', p_aggregate_type,
    'aggregateId', p_aggregate_id::text, 'requestId', p_request_id::text, 'expectedKind', p_expected_kind,
    'expectedRevision', p_expected_revision, 'events', p_events, 'context', p_context
  )::text, 'sha256');
  PERFORM pg_advisory_xact_lock(hashtextextended(p_producer_service || ':' || p_request_id::text, 0));
  SELECT request_sha256, response INTO v_existing_hash, v_existing_result FROM event_store.append_requests
    WHERE producer_service = p_producer_service AND request_id = p_request_id;
  IF FOUND THEN
    IF v_existing_hash <> v_request_hash THEN RAISE EXCEPTION 'idempotency key reused with different request' USING ERRCODE = '23505'; END IF;
    RETURN v_existing_result;
  END IF;

  v_recorded_at := date_trunc('milliseconds', clock_timestamp());
  v_recorded_text := to_char(v_recorded_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"');
  INSERT INTO event_store.streams(namespace, aggregate_type, aggregate_id, current_revision, created_at)
    VALUES (p_namespace, p_aggregate_type, p_aggregate_id, 0, v_recorded_at) ON CONFLICT DO NOTHING;
  GET DIAGNOSTICS v_created = ROW_COUNT;
  SELECT current_revision INTO STRICT v_current FROM event_store.streams
    WHERE namespace = p_namespace AND aggregate_type = p_aggregate_type AND aggregate_id = p_aggregate_id FOR UPDATE;
  IF p_expected_kind = 'no_stream' AND v_created = 0 THEN RAISE EXCEPTION 'stream already exists; actual revision %', v_current USING ERRCODE = '40001'; END IF;
  IF p_expected_kind = 'exact' AND v_current <> p_expected_revision THEN RAISE EXCEPTION 'expected revision %, actual revision %', p_expected_revision, v_current USING ERRCODE = '40001'; END IF;
  v_first := v_current + 1;

  FOR v_draft, v_ord IN SELECT value, ordinality::integer FROM jsonb_array_elements(p_events) WITH ORDINALITY LOOP
    IF jsonb_typeof(v_draft->'payload') <> 'object' OR (v_draft->>'eventName') IS NULL OR (v_draft->>'schemaVersion') IS NULL
      OR (v_draft->>'occurredAt') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$'
    THEN RAISE EXCEPTION 'invalid event draft at ordinal %', v_ord USING ERRCODE = '22023'; END IF;
    v_event_id := uuidv7(); v_event_number := nextval('event_store.event_number_seq'); v_revision := v_current + v_ord;
    v_occurred_at := (v_draft->>'occurredAt')::timestamptz;
    v_envelope := jsonb_build_object(
      'eventId', v_event_id::text, 'namespace', p_namespace, 'aggregateType', p_aggregate_type,
      'aggregateId', p_aggregate_id::text, 'streamRevision', v_revision::text, 'eventNumber', v_event_number::text,
      'eventName', v_draft->>'eventName', 'schemaVersion', (v_draft->>'schemaVersion')::integer,
      'occurredAt', v_draft->>'occurredAt', 'recordedAt', v_recorded_text, 'producerService', p_producer_service,
      'context', jsonb_strip_nulls(p_context || jsonb_build_object('requestId', p_request_id::text)), 'payload', v_draft->'payload'
    );
    v_envelope_hash := encode(digest(event_store.canonical_jsonb(v_envelope), 'sha256'), 'hex');
    INSERT INTO event_store.events(event_number,event_id,namespace,aggregate_type,aggregate_id,stream_revision,request_id,request_event_no,event_name,schema_version,occurred_at,recorded_at,producer_service,topic_route,partition_key,event_envelope,envelope_sha256)
    VALUES (v_event_number,v_event_id,p_namespace,p_aggregate_type,p_aggregate_id,v_revision,p_request_id,v_ord,v_draft->>'eventName',(v_draft->>'schemaVersion')::integer,v_occurred_at,v_recorded_at,p_producer_service,'event-store',p_namespace || '|' || p_aggregate_type || '|' || p_aggregate_id::text,v_envelope,v_envelope_hash);
    v_response_events := v_response_events || jsonb_build_array(jsonb_build_object('eventId', v_event_id::text, 'streamRevision', v_revision::text, 'eventNumber', v_event_number::text));
  END LOOP;
  v_last := v_current + jsonb_array_length(p_events);
  UPDATE event_store.streams SET current_revision = v_last, last_recorded_at = v_recorded_at WHERE namespace = p_namespace AND aggregate_type = p_aggregate_type AND aggregate_id = p_aggregate_id;
  v_response := jsonb_build_object('requestId',p_request_id::text,'namespace',p_namespace,'aggregateType',p_aggregate_type,'aggregateId',p_aggregate_id::text,'previousRevision',v_current::text,'currentRevision',v_last::text,'recordedAt',v_recorded_text,'events',v_response_events);
  INSERT INTO event_store.append_requests(producer_service,request_id,request_sha256,namespace,aggregate_type,aggregate_id,first_revision,last_revision,event_count,response,created_at)
    VALUES (p_producer_service,p_request_id,v_request_hash,p_namespace,p_aggregate_type,p_aggregate_id,v_first,v_last,jsonb_array_length(p_events),v_response,v_recorded_at);
  RETURN v_response;
END $$;
