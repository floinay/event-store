ALTER TABLE event_store.runtime_config
  ADD COLUMN cdc_delivery_startup_pending boolean NOT NULL DEFAULT false;

CREATE OR REPLACE FUNCTION event_store.set_cdc_delivery_health(p_healthy boolean)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
BEGIN
  IF p_healthy THEN
    RAISE EXCEPTION 'delivery health may only reopen with a timeline-bound proof' USING ERRCODE='42501';
  END IF;
  UPDATE event_store.runtime_config
     SET cdc_reconciliation_required=cdc_reconciliation_required OR cdc_delivery_healthy OR cdc_delivery_startup_pending,
         cdc_delivery_healthy=false,
         cdc_delivery_startup_pending=false
   WHERE singleton;
END $$;

CREATE OR REPLACE FUNCTION event_store.close_cdc_delivery_health_for_restart()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
BEGIN
  UPDATE event_store.runtime_config
     SET cdc_delivery_healthy=false,
         cdc_delivery_startup_pending=true
   WHERE singleton;
END $$;

CREATE OR REPLACE FUNCTION event_store.set_cdc_delivery_health_on_timeline(p_timeline_id integer)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_wal_budget_bytes bigint; v_previous_timeline_id integer; v_reconciliation_required boolean; v_reconciled boolean;
BEGIN
  IF p_timeline_id IS DISTINCT FROM event_store.current_timeline_id() THEN
    RAISE EXCEPTION 'CDC delivery proof belongs to a different promotion timeline' USING ERRCODE='P0001';
  END IF;
  SELECT wal_budget_bytes,cdc_delivery_timeline_id,cdc_reconciliation_required
    INTO v_wal_budget_bytes,v_previous_timeline_id,v_reconciliation_required
    FROM event_store.runtime_config WHERE singleton;
  IF v_reconciliation_required THEN
    RAISE EXCEPTION 'CDC delivery incident requires event-id reconciliation' USING ERRCODE='P0001';
  END IF;
  IF v_previous_timeline_id IS DISTINCT FROM p_timeline_id THEN
    SELECT true INTO v_reconciled FROM event_store.cdc_timeline_reconciliations WHERE timeline_id=p_timeline_id;
    IF v_reconciled IS DISTINCT FROM true THEN
      RAISE EXCEPTION 'new promotion timeline requires event-id reconciliation' USING ERRCODE='P0001';
    END IF;
  END IF;
  PERFORM event_store.assert_cdc_delivery_ready(v_wal_budget_bytes);
  UPDATE event_store.runtime_config
     SET cdc_delivery_healthy=true,cdc_delivery_timeline_id=p_timeline_id,cdc_delivery_startup_pending=false
   WHERE singleton;
END $$;
