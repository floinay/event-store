CREATE OR REPLACE FUNCTION event_store.enable_append_admission(p_wal_budget_bytes bigint)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
BEGIN
  IF p_wal_budget_bytes <= 0 THEN
    RAISE EXCEPTION 'WAL budget must be positive' USING ERRCODE = '22023';
  END IF;
  UPDATE event_store.runtime_config
    SET append_admission_enabled=true,
        wal_budget_bytes=p_wal_budget_bytes
    WHERE singleton;
END $$;
