-- This is intentionally narrower than append_v1: it emits one fixed internal
-- event so the service can prove Connect→Kafka delivery before reopening the
-- general append fence on a new primary or after an outage.
CREATE FUNCTION event_store.append_cdc_latency_probe(p_request_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_config event_store.runtime_config%ROWTYPE; v_occurred_at text;
BEGIN
  SELECT * INTO v_config FROM event_store.runtime_config WHERE singleton;
  IF v_config.append_admission_enabled THEN
    PERFORM event_store.assert_append_cdc_ready(v_config.wal_budget_bytes, false);
  END IF;
  v_occurred_at := to_char(
    date_trunc('milliseconds', clock_timestamp()) AT TIME ZONE 'UTC',
    'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'
  );
  RETURN event_store.append_v1_validated(
    'event-store-latency-probe',
    'system',
    'CdcLatencyProbe',
    p_request_id,
    p_request_id,
    'no_stream',
    NULL,
    jsonb_build_array(jsonb_build_object(
      'eventName', 'system.cdc.latency.probe',
      'schemaVersion', 1,
      'occurredAt', v_occurred_at,
      'payload', '{}'::jsonb
    )),
    jsonb_build_object(
      'requestId', p_request_id::text,
      'correlationId', p_request_id::text,
      'causationId', NULL,
      'actor', jsonb_build_object(
        'kind', 'service',
        'subjectRef', 'event-store-latency-probe'
      ),
      'trafficClass', 'standard'
    )
  );
END $$;

REVOKE ALL ON FUNCTION event_store.append_cdc_latency_probe(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.append_cdc_latency_probe(uuid) TO event_store_app;
