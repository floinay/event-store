ALTER TABLE event_store.runtime_config
  ADD COLUMN cdc_delivery_incident_epoch bigint NOT NULL DEFAULT 0,
  ADD COLUMN cdc_reconciled_incident_epoch bigint;

CREATE OR REPLACE FUNCTION event_store.set_cdc_delivery_health(p_healthy boolean)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
BEGIN
  IF p_healthy THEN RAISE EXCEPTION 'delivery health may only reopen with a timeline-bound proof' USING ERRCODE='42501'; END IF;
  UPDATE event_store.runtime_config
     SET cdc_reconciliation_required=cdc_reconciliation_required OR cdc_delivery_healthy OR cdc_delivery_startup_pending,
         cdc_delivery_incident_epoch=cdc_delivery_incident_epoch + CASE
           WHEN NOT cdc_reconciliation_required AND (cdc_delivery_healthy OR cdc_delivery_startup_pending) THEN 1 ELSE 0 END,
         cdc_delivery_healthy=false,cdc_delivery_startup_pending=false
   WHERE singleton;
END $$;

CREATE OR REPLACE FUNCTION event_store.record_cdc_timeline_reconciliation(
  p_projection_name text,p_generation_id uuid,p_timeline_id integer
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, event_store, projection_runtime AS $$
DECLARE v_missing bigint; v_unknown bigint; v_epoch bigint;
BEGIN
  IF p_timeline_id IS DISTINCT FROM event_store.current_timeline_id() THEN
    RAISE EXCEPTION 'reconciliation belongs to a different promotion timeline' USING ERRCODE='P0001';
  END IF;
  SELECT cdc_delivery_incident_epoch INTO v_epoch FROM event_store.runtime_config WHERE singleton FOR UPDATE;
  SELECT count(*) INTO v_missing FROM event_store.events e WHERE NOT EXISTS (
    SELECT 1 FROM projection_runtime.inbox i WHERE i.projection_name=p_projection_name AND i.generation_id=p_generation_id AND i.event_id=e.event_id);
  SELECT count(*) INTO v_unknown FROM projection_runtime.inbox i WHERE i.projection_name=p_projection_name AND i.generation_id=p_generation_id AND NOT EXISTS (
    SELECT 1 FROM event_store.events e WHERE e.event_id=i.event_id);
  IF v_missing <> 0 OR v_unknown <> 0 THEN RAISE EXCEPTION 'event-id reconciliation is incomplete' USING ERRCODE='P0001'; END IF;
  INSERT INTO event_store.cdc_timeline_reconciliations(timeline_id,projection_name,generation_id)
  VALUES (p_timeline_id,p_projection_name,p_generation_id)
  ON CONFLICT (timeline_id) DO UPDATE SET projection_name=EXCLUDED.projection_name,generation_id=EXCLUDED.generation_id,verified_at=clock_timestamp();
  UPDATE event_store.runtime_config SET cdc_reconciliation_required=false,cdc_reconciled_incident_epoch=v_epoch WHERE singleton;
END $$;

CREATE OR REPLACE FUNCTION event_store.set_cdc_delivery_health_on_timeline(p_timeline_id integer)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
DECLARE v_wal_budget_bytes bigint; v_previous_timeline_id integer; v_required boolean; v_epoch bigint; v_reconciled_epoch bigint; v_reconciled boolean;
BEGIN
  IF p_timeline_id IS DISTINCT FROM event_store.current_timeline_id() THEN RAISE EXCEPTION 'CDC delivery proof belongs to a different promotion timeline' USING ERRCODE='P0001'; END IF;
  SELECT wal_budget_bytes,cdc_delivery_timeline_id,cdc_reconciliation_required,cdc_delivery_incident_epoch,cdc_reconciled_incident_epoch
    INTO v_wal_budget_bytes,v_previous_timeline_id,v_required,v_epoch,v_reconciled_epoch FROM event_store.runtime_config WHERE singleton FOR UPDATE;
  IF v_required OR (v_epoch > 0 AND v_reconciled_epoch IS DISTINCT FROM v_epoch) THEN RAISE EXCEPTION 'CDC delivery incident requires event-id reconciliation' USING ERRCODE='P0001'; END IF;
  IF v_previous_timeline_id IS DISTINCT FROM p_timeline_id THEN
    SELECT true INTO v_reconciled FROM event_store.cdc_timeline_reconciliations WHERE timeline_id=p_timeline_id;
    IF v_reconciled IS DISTINCT FROM true THEN RAISE EXCEPTION 'new promotion timeline requires event-id reconciliation' USING ERRCODE='P0001'; END IF;
  END IF;
  PERFORM event_store.assert_cdc_delivery_ready(v_wal_budget_bytes);
  UPDATE event_store.runtime_config SET cdc_delivery_healthy=true,cdc_delivery_timeline_id=p_timeline_id,cdc_delivery_startup_pending=false WHERE singleton;
END $$;
