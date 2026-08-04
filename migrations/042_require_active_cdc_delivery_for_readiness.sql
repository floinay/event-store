CREATE FUNCTION event_store.assert_cdc_delivery_ready(p_wal_budget_bytes bigint)
RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE
  v_slot_name text;
  v_slot_active boolean;
  v_tables text[];
BEGIN
  PERFORM event_store.assert_append_cdc_ready(p_wal_budget_bytes, false);
  SELECT cdc_slot_name INTO v_slot_name
    FROM event_store.runtime_config WHERE singleton;
  SELECT active INTO v_slot_active
    FROM pg_replication_slots WHERE slot_name = v_slot_name;
  IF v_slot_active IS DISTINCT FROM true THEN
    RAISE EXCEPTION 'CDC slot is not actively delivering' USING ERRCODE='P0001';
  END IF;
  SELECT array_agg(schemaname || '.' || tablename ORDER BY schemaname, tablename)
    INTO v_tables
    FROM pg_publication_tables
   WHERE pubname = 'event_store_events';
  IF v_tables IS DISTINCT FROM ARRAY['event_store.events'] THEN
    RAISE EXCEPTION 'CDC publication is not restricted to event_store.events'
      USING ERRCODE='P0001';
  END IF;
END $$;

REVOKE ALL ON FUNCTION event_store.assert_cdc_delivery_ready(bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.assert_cdc_delivery_ready(bigint) TO event_store_app;
