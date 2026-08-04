CREATE FUNCTION event_store.contains_json_number(p_value jsonb)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path = pg_catalog, event_store
AS $$
  SELECT CASE jsonb_typeof($1)
    WHEN 'number' THEN true
    WHEN 'array' THEN EXISTS (
      SELECT 1 FROM jsonb_array_elements($1) AS values(value)
      WHERE event_store.contains_json_number(value)
    )
    WHEN 'object' THEN EXISTS (
      SELECT 1 FROM jsonb_each($1) AS values(key, value)
      WHERE event_store.contains_json_number(value)
    )
    ELSE false
  END
$$;

ALTER FUNCTION event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)
  RENAME TO append_v1_validated;
REVOKE ALL ON FUNCTION event_store.append_v1_validated(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)
  FROM PUBLIC, event_store_app, event_store_cdc, projection_worker;

CREATE FUNCTION event_store.append_v1(
  p_producer_service text, p_namespace text, p_aggregate_type text, p_aggregate_id uuid,
  p_request_id uuid, p_expected_kind text, p_expected_revision bigint, p_events jsonb, p_context jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
BEGIN
  IF event_store.contains_json_number(p_events) OR event_store.contains_json_number(p_context) THEN
    RAISE EXCEPTION 'JSON numbers are prohibited; use decimal strings' USING ERRCODE = '22023';
  END IF;
  RETURN event_store.append_v1_validated(
    p_producer_service, p_namespace, p_aggregate_type, p_aggregate_id,
    p_request_id, p_expected_kind, p_expected_revision, p_events, p_context
  );
END $$;

REVOKE ALL ON FUNCTION event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)
  FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)
  TO event_store_app;
