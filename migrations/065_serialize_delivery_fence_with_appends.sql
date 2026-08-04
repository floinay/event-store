CREATE OR REPLACE FUNCTION event_store.append_v1(
  p_producer_service text, p_namespace text, p_aggregate_type text, p_aggregate_id uuid,
  p_request_id uuid, p_expected_kind text, p_expected_revision bigint, p_events jsonb, p_context jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
DECLARE v_config event_store.runtime_config%ROWTYPE;
BEGIN
  IF event_store.contains_direct_pii(p_events) OR event_store.contains_direct_pii(p_context) THEN
    RAISE EXCEPTION 'direct PII field is prohibited' USING ERRCODE = '22023';
  END IF;
  IF event_store.contains_unrepresentable_json_number(p_events)
     OR event_store.contains_unrepresentable_json_number(p_context) THEN
    RAISE EXCEPTION 'JSON number is not representable by canonical JavaScript consumer' USING ERRCODE = '22023';
  END IF;
  SELECT * INTO v_config FROM event_store.runtime_config WHERE singleton FOR SHARE;
  IF v_config.append_admission_enabled THEN
    IF v_config.cdc_delivery_healthy IS DISTINCT FROM true THEN
      RAISE EXCEPTION 'CDC delivery health has closed append admission' USING ERRCODE='P0001';
    END IF;
    PERFORM event_store.assert_cdc_delivery_timeline(v_config.cdc_delivery_timeline_id);
    PERFORM event_store.assert_append_cdc_ready(v_config.wal_budget_bytes, false);
  END IF;
  RETURN event_store.append_v1_validated(
    p_producer_service, p_namespace, p_aggregate_type, p_aggregate_id,
    p_request_id, p_expected_kind, p_expected_revision, p_events, p_context
  );
END $$;

CREATE OR REPLACE FUNCTION event_store.append_v1_critical(
  p_producer_service text, p_namespace text, p_aggregate_type text, p_aggregate_id uuid,
  p_request_id uuid, p_expected_kind text, p_expected_revision bigint, p_events jsonb, p_context jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
DECLARE v_config event_store.runtime_config%ROWTYPE;
BEGIN
  IF session_user <> 'event_store_critical_app' THEN
    RAISE EXCEPTION 'critical append principal is required' USING ERRCODE='42501';
  END IF;
  IF event_store.contains_direct_pii(p_events) OR event_store.contains_direct_pii(p_context) THEN
    RAISE EXCEPTION 'direct PII field is prohibited' USING ERRCODE = '22023';
  END IF;
  IF event_store.contains_unrepresentable_json_number(p_events)
     OR event_store.contains_unrepresentable_json_number(p_context) THEN
    RAISE EXCEPTION 'JSON number is not representable by canonical JavaScript consumer' USING ERRCODE = '22023';
  END IF;
  SELECT * INTO v_config FROM event_store.runtime_config WHERE singleton FOR SHARE;
  IF v_config.append_admission_enabled THEN
    IF v_config.cdc_delivery_healthy IS DISTINCT FROM true THEN
      RAISE EXCEPTION 'CDC delivery health has closed append admission' USING ERRCODE='P0001';
    END IF;
    PERFORM event_store.assert_cdc_delivery_timeline(v_config.cdc_delivery_timeline_id);
    PERFORM event_store.assert_append_cdc_ready(v_config.wal_budget_bytes, true);
  END IF;
  RETURN event_store.append_v1_validated(
    p_producer_service, p_namespace, p_aggregate_type, p_aggregate_id,
    p_request_id, p_expected_kind, p_expected_revision, p_events, p_context
  );
END $$;

CREATE OR REPLACE FUNCTION event_store.append_cdc_latency_probe(p_request_id uuid)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_config event_store.runtime_config%ROWTYPE; v_occurred_at text;
BEGIN
  SELECT * INTO v_config FROM event_store.runtime_config WHERE singleton FOR SHARE;
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
