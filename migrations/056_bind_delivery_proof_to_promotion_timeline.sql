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
     SET cdc_delivery_healthy=false
   WHERE singleton;
END $$;

CREATE FUNCTION event_store.set_cdc_delivery_health_on_timeline(p_timeline_id integer)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_wal_budget_bytes bigint;
BEGIN
  IF p_timeline_id IS DISTINCT FROM event_store.current_timeline_id() THEN
    RAISE EXCEPTION 'CDC delivery proof belongs to a different promotion timeline'
      USING ERRCODE='P0001';
  END IF;
  SELECT wal_budget_bytes INTO v_wal_budget_bytes
    FROM event_store.runtime_config WHERE singleton;
  PERFORM event_store.assert_cdc_delivery_ready(v_wal_budget_bytes);
  UPDATE event_store.runtime_config
     SET cdc_delivery_healthy=true,
         cdc_delivery_timeline_id=p_timeline_id
   WHERE singleton;
END $$;

REVOKE ALL ON FUNCTION event_store.set_cdc_delivery_health_on_timeline(integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.set_cdc_delivery_health_on_timeline(integer) TO event_store_app;
