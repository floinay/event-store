ALTER TABLE event_store.runtime_config
  ADD COLUMN cdc_reconciliation_required boolean NOT NULL DEFAULT false;

CREATE OR REPLACE FUNCTION event_store.set_cdc_delivery_health(p_healthy boolean)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
BEGIN
  IF p_healthy THEN
    RAISE EXCEPTION 'delivery health may only reopen with a timeline-bound proof'
      USING ERRCODE='42501';
  END IF;
  UPDATE event_store.runtime_config
     SET cdc_delivery_healthy=false,
         cdc_reconciliation_required=cdc_reconciliation_required OR cdc_delivery_healthy
   WHERE singleton;
END $$;

CREATE FUNCTION event_store.close_cdc_delivery_health_for_restart()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
BEGIN
  UPDATE event_store.runtime_config
     SET cdc_reconciliation_required=cdc_reconciliation_required OR NOT cdc_delivery_healthy,
         cdc_delivery_healthy=false
   WHERE singleton;
END $$;

REVOKE ALL ON FUNCTION event_store.close_cdc_delivery_health_for_restart() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.close_cdc_delivery_health_for_restart() TO event_store_app;

CREATE OR REPLACE FUNCTION event_store.record_cdc_timeline_reconciliation(
  p_projection_name text,
  p_generation_id uuid,
  p_timeline_id integer
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store, projection_runtime
AS $$
DECLARE v_missing bigint; v_unknown bigint;
BEGIN
  IF p_timeline_id IS DISTINCT FROM event_store.current_timeline_id() THEN
    RAISE EXCEPTION 'reconciliation belongs to a different promotion timeline' USING ERRCODE='P0001';
  END IF;
  SELECT count(*) INTO v_missing FROM event_store.events e WHERE NOT EXISTS (
    SELECT 1 FROM projection_runtime.inbox i WHERE i.projection_name=p_projection_name
      AND i.generation_id=p_generation_id AND i.event_id=e.event_id);
  SELECT count(*) INTO v_unknown FROM projection_runtime.inbox i WHERE i.projection_name=p_projection_name
    AND i.generation_id=p_generation_id AND NOT EXISTS (
      SELECT 1 FROM event_store.events e WHERE e.event_id=i.event_id);
  IF v_missing <> 0 OR v_unknown <> 0 THEN
    RAISE EXCEPTION 'event-id reconciliation is incomplete' USING ERRCODE='P0001';
  END IF;
  INSERT INTO event_store.cdc_timeline_reconciliations(timeline_id,projection_name,generation_id)
  VALUES (p_timeline_id,p_projection_name,p_generation_id)
  ON CONFLICT (timeline_id) DO UPDATE SET projection_name=EXCLUDED.projection_name,
    generation_id=EXCLUDED.generation_id,verified_at=clock_timestamp();
  UPDATE event_store.runtime_config SET cdc_reconciliation_required=false WHERE singleton;
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
    SELECT true INTO v_reconciled FROM event_store.cdc_timeline_reconciliations
      WHERE timeline_id=p_timeline_id;
    IF v_reconciled IS DISTINCT FROM true THEN
      RAISE EXCEPTION 'new promotion timeline requires event-id reconciliation' USING ERRCODE='P0001';
    END IF;
  END IF;
  PERFORM event_store.assert_cdc_delivery_ready(v_wal_budget_bytes);
  UPDATE event_store.runtime_config SET cdc_delivery_healthy=true,cdc_delivery_timeline_id=p_timeline_id WHERE singleton;
END $$;
