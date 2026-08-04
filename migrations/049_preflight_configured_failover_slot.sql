CREATE FUNCTION event_store.assert_configured_failover_candidate()
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_slot_name text;
BEGIN
  SELECT cdc_slot_name INTO v_slot_name
    FROM event_store.runtime_config
   WHERE singleton
   FOR KEY SHARE;
  IF v_slot_name IS NULL THEN
    RAISE EXCEPTION 'configured CDC slot is missing' USING ERRCODE = 'P0001';
  END IF;
  PERFORM event_store.assert_failover_candidate(v_slot_name);
END $$;

REVOKE ALL ON FUNCTION event_store.assert_configured_failover_candidate() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.assert_configured_failover_candidate() TO event_store_cdc;
