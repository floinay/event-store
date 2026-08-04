CREATE FUNCTION event_store.assert_recovery_cdc_slot_ready(p_slot_name text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_slot record;
BEGIN
  SELECT active,failover,synced,temporary,invalidation_reason,restart_lsn
    INTO v_slot
    FROM pg_replication_slots WHERE slot_name=p_slot_name;
  IF NOT FOUND OR NOT v_slot.active OR NOT v_slot.failover
     OR (pg_is_in_recovery() AND v_slot.synced IS DISTINCT FROM true)
     OR v_slot.temporary OR v_slot.invalidation_reason IS NOT NULL
     OR v_slot.restart_lsn IS NULL THEN
    RAISE EXCEPTION 'recovery CDC slot is not failover-ready' USING ERRCODE='P0001';
  END IF;
END $$;

REVOKE ALL ON FUNCTION event_store.assert_recovery_cdc_slot_ready(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.assert_recovery_cdc_slot_ready(text) TO event_store_cdc;

CREATE OR REPLACE FUNCTION event_store.activate_recovery_cdc_slot(
  p_slot_name text,
  p_connector_name text,
  p_wal_budget_bytes bigint
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_verified boolean;
BEGIN
  IF p_slot_name !~ '^event_store_[a-z0-9_]{1,50}$'
     OR p_connector_name !~ '^event-store-[a-z0-9-]{1,63}$' THEN
    RAISE EXCEPTION 'invalid recovery CDC identity' USING ERRCODE = '22023';
  END IF;
  IF p_wal_budget_bytes <= 0 THEN
    RAISE EXCEPTION 'WAL budget must be positive' USING ERRCODE = '22023';
  END IF;
  PERFORM event_store.assert_recovery_cdc_slot_ready(p_slot_name);
  SELECT true INTO v_verified
    FROM event_store.recovery_cdc_verifications
   WHERE slot_name=p_slot_name
     AND connector_name=p_connector_name
     AND kafka_lag=0;
  IF v_verified IS DISTINCT FROM true THEN
    RAISE EXCEPTION 'recovery CDC cutover has not been verified' USING ERRCODE = 'P0001';
  END IF;
  UPDATE event_store.runtime_config
     SET append_admission_enabled=true,
         cdc_delivery_healthy=true,
         cdc_delivery_timeline_id=event_store.current_timeline_id(),
         wal_budget_bytes=p_wal_budget_bytes,
         cdc_slot_name=p_slot_name,
         cdc_connector_name=p_connector_name
   WHERE singleton;
END $$;
