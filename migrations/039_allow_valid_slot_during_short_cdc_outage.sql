CREATE OR REPLACE FUNCTION event_store.assert_append_cdc_ready(p_wal_budget_bytes bigint)
RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_slot record; v_retained numeric; v_slot_name text;
BEGIN
  IF p_wal_budget_bytes <= 0 THEN
    RAISE EXCEPTION 'WAL budget must be positive' USING ERRCODE='22023';
  END IF;
  SELECT cdc_slot_name INTO v_slot_name FROM event_store.runtime_config WHERE singleton;
  SELECT failover, temporary, invalidation_reason, restart_lsn INTO v_slot
    FROM pg_replication_slots WHERE slot_name=v_slot_name;
  -- Temporary Connect outages retain WAL behind a valid slot. Readiness is
  -- unhealthy, but appends remain available until the WAL admission threshold.
  IF NOT FOUND OR NOT v_slot.failover OR v_slot.temporary
     OR v_slot.invalidation_reason IS NOT NULL OR v_slot.restart_lsn IS NULL THEN
    RAISE EXCEPTION 'CDC slot is not ready for append' USING ERRCODE='P0001';
  END IF;
  v_retained := pg_wal_lsn_diff(pg_current_wal_lsn(), v_slot.restart_lsn);
  IF v_retained >= p_wal_budget_bytes * 0.85 THEN
    RAISE EXCEPTION 'CDC retained WAL exceeds append admission threshold' USING ERRCODE='P0001';
  END IF;
END $$;
