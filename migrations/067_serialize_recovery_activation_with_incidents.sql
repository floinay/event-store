CREATE OR REPLACE FUNCTION event_store.activate_recovery_cdc_slot(
  p_slot_name text,
  p_connector_name text,
  p_wal_budget_bytes bigint
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE
  v_verified boolean;
  v_timeline_id integer;
  v_reconciliation_required boolean;
  v_incident_epoch bigint;
  v_reconciled_epoch bigint;
BEGIN
  IF p_slot_name !~ '^event_store_[a-z0-9_]{1,50}$'
     OR p_connector_name !~ '^event-store-[a-z0-9-]{1,63}$' THEN
    RAISE EXCEPTION 'invalid recovery CDC identity' USING ERRCODE = '22023';
  END IF;
  IF p_wal_budget_bytes <= 0 THEN
    RAISE EXCEPTION 'WAL budget must be positive' USING ERRCODE = '22023';
  END IF;
  -- Serialize activation with a delivery failure. A failure before this lock
  -- is rejected below; a failure waiting on it fences writes immediately after.
  SELECT cdc_reconciliation_required,cdc_delivery_incident_epoch,cdc_reconciled_incident_epoch
    INTO v_reconciliation_required,v_incident_epoch,v_reconciled_epoch
    FROM event_store.runtime_config WHERE singleton FOR UPDATE;
  IF v_reconciliation_required
     OR (v_incident_epoch > 0 AND v_reconciled_epoch IS DISTINCT FROM v_incident_epoch) THEN
    RAISE EXCEPTION 'recovery activation requires delivery incident reconciliation'
      USING ERRCODE='P0001';
  END IF;
  PERFORM event_store.assert_recovery_cdc_slot_ready(p_slot_name);
  v_timeline_id := event_store.current_timeline_id();
  SELECT true INTO v_verified
    FROM event_store.recovery_cdc_verifications
   WHERE slot_name=p_slot_name
     AND connector_name=p_connector_name
     AND kafka_lag=0
     AND verified_timeline_id=v_timeline_id;
  IF v_verified IS DISTINCT FROM true THEN
    RAISE EXCEPTION 'recovery CDC cutover has not been verified on the current timeline' USING ERRCODE = 'P0001';
  END IF;
  UPDATE event_store.runtime_config
     SET append_admission_enabled=true,
         cdc_delivery_healthy=true,
         cdc_delivery_timeline_id=v_timeline_id,
         wal_budget_bytes=p_wal_budget_bytes,
         cdc_slot_name=p_slot_name,
         cdc_connector_name=p_connector_name
   WHERE singleton;
END $$;
