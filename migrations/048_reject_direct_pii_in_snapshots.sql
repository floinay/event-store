CREATE OR REPLACE FUNCTION event_store.put_snapshot_v1(
  p_namespace text, p_aggregate_type text, p_aggregate_id uuid, p_snapshot_revision bigint,
  p_reducer_version text, p_state_schema_version integer, p_state jsonb, p_state_sha256 bytea
) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
DECLARE v_existing bytea;
BEGIN
  IF jsonb_typeof(p_state) <> 'object' THEN
    RAISE EXCEPTION 'snapshot state must be an object' USING ERRCODE = '22023';
  END IF;
  IF event_store.contains_direct_pii(p_state) THEN
    RAISE EXCEPTION 'direct PII field is prohibited' USING ERRCODE = '22023';
  END IF;
  INSERT INTO event_store.snapshots(namespace, aggregate_type, aggregate_id, snapshot_revision, reducer_version, state_schema_version, state, state_sha256, created_at)
  VALUES (p_namespace, p_aggregate_type, p_aggregate_id, p_snapshot_revision, p_reducer_version, p_state_schema_version, p_state, p_state_sha256, clock_timestamp())
  ON CONFLICT (namespace, aggregate_type, aggregate_id, snapshot_revision, reducer_version) DO NOTHING;
  IF FOUND THEN
    DELETE FROM event_store.snapshots WHERE ctid IN (
      SELECT ctid FROM event_store.snapshots WHERE namespace=p_namespace AND aggregate_type=p_aggregate_type AND aggregate_id=p_aggregate_id AND reducer_version=p_reducer_version
      ORDER BY snapshot_revision DESC OFFSET 2
    );
    RETURN;
  END IF;
  SELECT state_sha256 INTO v_existing FROM event_store.snapshots WHERE namespace=p_namespace AND aggregate_type=p_aggregate_type AND aggregate_id=p_aggregate_id AND snapshot_revision=p_snapshot_revision AND reducer_version=p_reducer_version;
  IF v_existing <> p_state_sha256 THEN RAISE EXCEPTION 'snapshot nondeterminism' USING ERRCODE = 'XX001'; END IF;
END $$;
