CREATE FUNCTION event_store.assert_append_cdc_ready(p_wal_budget_bytes bigint)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE
  v_slot record;
  v_retained numeric;
BEGIN
  IF p_wal_budget_bytes <= 0 THEN
    RAISE EXCEPTION 'WAL budget must be positive' USING ERRCODE = '22023';
  END IF;
  SELECT active, invalidation_reason, restart_lsn
    INTO v_slot
    FROM pg_replication_slots
    WHERE slot_name = 'event_store_live';
  IF NOT FOUND OR NOT v_slot.active OR v_slot.invalidation_reason IS NOT NULL OR v_slot.restart_lsn IS NULL THEN
    RAISE EXCEPTION 'CDC slot is not ready for append' USING ERRCODE = 'P0001';
  END IF;
  v_retained := pg_wal_lsn_diff(pg_current_wal_lsn(), v_slot.restart_lsn);
  IF v_retained >= p_wal_budget_bytes * 0.85 THEN
    RAISE EXCEPTION 'CDC retained WAL exceeds append admission threshold' USING ERRCODE = 'P0001';
  END IF;
END $$;

REVOKE ALL ON FUNCTION event_store.assert_append_cdc_ready(bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.assert_append_cdc_ready(bigint) TO event_store_app;
