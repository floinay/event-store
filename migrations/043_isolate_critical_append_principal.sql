GRANT CONNECT ON DATABASE event_store TO event_store_critical_app;
GRANT USAGE ON SCHEMA event_store TO event_store_critical_app;

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
  SELECT * INTO v_config FROM event_store.runtime_config WHERE singleton;
  IF v_config.append_admission_enabled THEN
    PERFORM event_store.assert_append_cdc_ready(v_config.wal_budget_bytes, false);
  END IF;
  RETURN event_store.append_v1_validated(
    p_producer_service, p_namespace, p_aggregate_type, p_aggregate_id,
    p_request_id, p_expected_kind, p_expected_revision, p_events, p_context
  );
END $$;

CREATE FUNCTION event_store.append_v1_critical(
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
  SELECT * INTO v_config FROM event_store.runtime_config WHERE singleton;
  IF v_config.append_admission_enabled THEN
    PERFORM event_store.assert_append_cdc_ready(v_config.wal_budget_bytes, true);
  END IF;
  RETURN event_store.append_v1_validated(
    p_producer_service, p_namespace, p_aggregate_type, p_aggregate_id,
    p_request_id, p_expected_kind, p_expected_revision, p_events, p_context
  );
END $$;

REVOKE ALL ON FUNCTION event_store.append_v1_critical(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.append_v1_critical(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb) TO event_store_critical_app;
