CREATE INDEX projection_failures_retention_idx
  ON projection_runtime.failures (last_failed_at);

CREATE FUNCTION projection_runtime.prune_failures()
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, projection_runtime
AS $$
DECLARE
  v_deleted bigint;
BEGIN
  DELETE FROM projection_runtime.failures
   WHERE last_failed_at < clock_timestamp() - interval '30 days';
  GET DIAGNOSTICS v_deleted = ROW_COUNT;
  RETURN v_deleted;
END;
$$;

REVOKE ALL ON FUNCTION projection_runtime.prune_failures() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION projection_runtime.prune_failures()
  TO projection_worker, event_store_app;
