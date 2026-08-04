CREATE FUNCTION event_store.assert_failover_candidate(p_slot_name text)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_slot record;
BEGIN
  IF p_slot_name !~ '^event_store_[a-z0-9_]{1,50}$' THEN
    RAISE EXCEPTION 'invalid CDC slot name' USING ERRCODE = '22023';
  END IF;
  SELECT failover, synced, temporary, invalidation_reason
    INTO v_slot
    FROM pg_replication_slots
   WHERE slot_name = p_slot_name;
  IF NOT FOUND
     OR NOT v_slot.failover
     OR NOT v_slot.synced
     OR v_slot.temporary
     OR v_slot.invalidation_reason IS NOT NULL
  THEN
    RAISE EXCEPTION 'failover candidate CDC slot is not synchronized'
      USING ERRCODE = 'P0001';
  END IF;
END $$;

REVOKE ALL ON FUNCTION event_store.assert_failover_candidate(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.assert_failover_candidate(text) TO event_store_cdc;
