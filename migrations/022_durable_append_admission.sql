CREATE TABLE event_store.runtime_config (
  singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
  append_admission_enabled boolean NOT NULL DEFAULT false,
  wal_budget_bytes bigint NOT NULL DEFAULT 8589934592 CHECK (wal_budget_bytes > 0)
);
INSERT INTO event_store.runtime_config(singleton) VALUES (true) ON CONFLICT DO NOTHING;

CREATE FUNCTION event_store.enable_append_admission(p_wal_budget_bytes bigint)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
BEGIN
  IF p_wal_budget_bytes <= 0 THEN
    RAISE EXCEPTION 'WAL budget must be positive' USING ERRCODE = '22023';
  END IF;
  UPDATE event_store.runtime_config
    SET append_admission_enabled=true,wal_budget_bytes=p_wal_budget_bytes
    WHERE singleton;
END $$;
REVOKE ALL ON FUNCTION event_store.enable_append_admission(bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.enable_append_admission(bigint) TO event_store_cdc;

ALTER FUNCTION event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)
  RENAME TO append_v1_without_admission;
REVOKE ALL ON FUNCTION event_store.append_v1_without_admission(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)
  FROM PUBLIC, event_store_app, event_store_cdc, projection_worker;

CREATE FUNCTION event_store.append_v1(
  p_producer_service text, p_namespace text, p_aggregate_type text, p_aggregate_id uuid,
  p_request_id uuid, p_expected_kind text, p_expected_revision bigint, p_events jsonb, p_context jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
DECLARE v_config event_store.runtime_config%ROWTYPE;
BEGIN
  SELECT * INTO v_config FROM event_store.runtime_config WHERE singleton;
  IF v_config.append_admission_enabled THEN
    PERFORM event_store.assert_append_cdc_ready(v_config.wal_budget_bytes);
  END IF;
  RETURN event_store.append_v1_validated(
    p_producer_service, p_namespace, p_aggregate_type, p_aggregate_id,
    p_request_id, p_expected_kind, p_expected_revision, p_events, p_context
  );
END $$;
REVOKE ALL ON FUNCTION event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb) TO event_store_app;
