ALTER TABLE event_store.runtime_config
  ADD COLUMN cdc_slot_name text NOT NULL DEFAULT 'event_store_live'
  CHECK (cdc_slot_name ~ '^event_store_[a-z0-9_]{1,50}$');

CREATE OR REPLACE FUNCTION event_store.enable_append_admission(p_wal_budget_bytes bigint)
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
    SET append_admission_enabled=true,
        wal_budget_bytes=p_wal_budget_bytes,
        cdc_slot_name='event_store_live'
    WHERE singleton;
END $$;

CREATE FUNCTION event_store.activate_recovery_cdc_slot(
  p_slot_name text,
  p_wal_budget_bytes bigint
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_slot record;
BEGIN
  IF p_slot_name !~ '^event_store_[a-z0-9_]{1,50}$' THEN
    RAISE EXCEPTION 'invalid CDC slot name' USING ERRCODE = '22023';
  END IF;
  IF p_wal_budget_bytes <= 0 THEN
    RAISE EXCEPTION 'WAL budget must be positive' USING ERRCODE = '22023';
  END IF;
  SELECT active, invalidation_reason, restart_lsn INTO v_slot
    FROM pg_replication_slots WHERE slot_name=p_slot_name;
  IF NOT FOUND OR NOT v_slot.active OR v_slot.invalidation_reason IS NOT NULL OR v_slot.restart_lsn IS NULL THEN
    RAISE EXCEPTION 'recovery CDC slot is not ready' USING ERRCODE = 'P0001';
  END IF;
  UPDATE event_store.runtime_config
    SET append_admission_enabled=true,
        wal_budget_bytes=p_wal_budget_bytes,
        cdc_slot_name=p_slot_name
    WHERE singleton;
END $$;

CREATE OR REPLACE FUNCTION event_store.assert_append_cdc_ready(p_wal_budget_bytes bigint)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_slot record; v_retained numeric; v_slot_name text;
BEGIN
  IF p_wal_budget_bytes <= 0 THEN
    RAISE EXCEPTION 'WAL budget must be positive' USING ERRCODE = '22023';
  END IF;
  SELECT cdc_slot_name INTO v_slot_name
    FROM event_store.runtime_config WHERE singleton;
  SELECT active, invalidation_reason, restart_lsn INTO v_slot
    FROM pg_replication_slots WHERE slot_name=v_slot_name;
  IF NOT FOUND OR NOT v_slot.active OR v_slot.invalidation_reason IS NOT NULL OR v_slot.restart_lsn IS NULL THEN
    RAISE EXCEPTION 'CDC slot is not ready for append' USING ERRCODE = 'P0001';
  END IF;
  v_retained := pg_wal_lsn_diff(pg_current_wal_lsn(), v_slot.restart_lsn);
  IF v_retained >= p_wal_budget_bytes * 0.85 THEN
    RAISE EXCEPTION 'CDC retained WAL exceeds append admission threshold' USING ERRCODE = 'P0001';
  END IF;
END $$;

REVOKE ALL ON FUNCTION event_store.activate_recovery_cdc_slot(text,bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.activate_recovery_cdc_slot(text,bigint) TO event_store_cdc;
