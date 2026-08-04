CREATE FUNCTION event_store.assert_append_cdc_ready(
  p_wal_budget_bytes bigint,
  p_critical boolean
)
RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_slot record; v_retained numeric; v_slot_name text; v_bootstrap_complete boolean; v_threshold numeric;
BEGIN
  IF p_wal_budget_bytes <= 0 THEN
    RAISE EXCEPTION 'WAL budget must be positive' USING ERRCODE='22023';
  END IF;
  SELECT cdc_slot_name, cdc_bootstrap_complete INTO v_slot_name, v_bootstrap_complete
    FROM event_store.runtime_config WHERE singleton;
  IF v_bootstrap_complete IS DISTINCT FROM true THEN
    RAISE EXCEPTION 'CDC bootstrap is incomplete' USING ERRCODE='P0001';
  END IF;
  SELECT failover, temporary, invalidation_reason, restart_lsn INTO v_slot
    FROM pg_replication_slots WHERE slot_name=v_slot_name;
  IF NOT FOUND OR NOT v_slot.failover OR v_slot.temporary
     OR v_slot.invalidation_reason IS NOT NULL OR v_slot.restart_lsn IS NULL THEN
    RAISE EXCEPTION 'CDC slot is not ready for append' USING ERRCODE='P0001';
  END IF;
  v_retained := pg_wal_lsn_diff(pg_current_wal_lsn(), v_slot.restart_lsn);
  v_threshold := CASE WHEN p_critical THEN 0.85 ELSE 0.70 END;
  IF v_retained >= p_wal_budget_bytes * v_threshold THEN
    RAISE EXCEPTION 'CDC retained WAL exceeds append admission threshold' USING ERRCODE='P0001';
  END IF;
END $$;

CREATE OR REPLACE FUNCTION event_store.append_v1(
  p_producer_service text, p_namespace text, p_aggregate_type text, p_aggregate_id uuid,
  p_request_id uuid, p_expected_kind text, p_expected_revision bigint, p_events jsonb, p_context jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
DECLARE v_config event_store.runtime_config%ROWTYPE;
BEGIN
  SELECT * INTO v_config FROM event_store.runtime_config WHERE singleton;
  IF v_config.append_admission_enabled THEN
    PERFORM event_store.assert_append_cdc_ready(
      v_config.wal_budget_bytes,
      COALESCE(p_context->>'trafficClass', 'standard') = 'critical'
    );
  END IF;
  RETURN event_store.append_v1_validated(
    p_producer_service, p_namespace, p_aggregate_type, p_aggregate_id,
    p_request_id, p_expected_kind, p_expected_revision, p_events, p_context
  );
END $$;
REVOKE ALL ON FUNCTION event_store.assert_append_cdc_ready(bigint,boolean) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.assert_append_cdc_ready(bigint,boolean) TO event_store_app;
