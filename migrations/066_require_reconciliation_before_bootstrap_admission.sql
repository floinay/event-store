CREATE OR REPLACE FUNCTION event_store.enable_append_admission(p_wal_budget_bytes bigint)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE
  v_previous_timeline_id integer;
  v_current_timeline_id integer;
  v_reconciliation_required boolean;
  v_incident_epoch bigint;
  v_reconciled_epoch bigint;
BEGIN
  IF p_wal_budget_bytes <= 0 THEN
    RAISE EXCEPTION 'WAL budget must be positive' USING ERRCODE = '22023';
  END IF;
  SELECT cdc_delivery_timeline_id,cdc_reconciliation_required,
         cdc_delivery_incident_epoch,cdc_reconciled_incident_epoch
    INTO v_previous_timeline_id,v_reconciliation_required,v_incident_epoch,v_reconciled_epoch
    FROM event_store.runtime_config WHERE singleton FOR UPDATE;
  v_current_timeline_id := event_store.current_timeline_id();
  IF v_previous_timeline_id IS NOT NULL
     AND v_previous_timeline_id IS DISTINCT FROM v_current_timeline_id THEN
    RAISE EXCEPTION 'bootstrap admission cannot bypass promotion reconciliation'
      USING ERRCODE='P0001';
  END IF;
  IF v_reconciliation_required
     OR (v_incident_epoch > 0 AND v_reconciled_epoch IS DISTINCT FROM v_incident_epoch) THEN
    RAISE EXCEPTION 'bootstrap admission cannot bypass delivery incident reconciliation'
      USING ERRCODE='P0001';
  END IF;
  UPDATE event_store.runtime_config
     SET append_admission_enabled=true,
         cdc_bootstrap_complete=true,
         cdc_delivery_healthy=true,
         cdc_delivery_timeline_id=v_current_timeline_id,
         wal_budget_bytes=p_wal_budget_bytes
   WHERE singleton;
END $$;
