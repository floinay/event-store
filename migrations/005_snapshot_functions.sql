CREATE OR REPLACE FUNCTION event_store.put_snapshot_v1(
  p_namespace text, p_aggregate_type text, p_aggregate_id uuid, p_snapshot_revision bigint,
  p_reducer_version text, p_state_schema_version integer, p_state jsonb, p_state_sha256 bytea
) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
DECLARE v_existing bytea;
BEGIN
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

CREATE OR REPLACE FUNCTION event_store.get_stream_head_v1(p_namespace text, p_aggregate_type text, p_aggregate_id uuid)
RETURNS bigint LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
  SELECT current_revision FROM event_store.streams WHERE namespace=$1 AND aggregate_type=$2 AND aggregate_id=$3
$$;

CREATE OR REPLACE FUNCTION event_store.read_stream_v1(p_namespace text, p_aggregate_type text, p_aggregate_id uuid, p_from_revision bigint DEFAULT 1)
RETURNS SETOF jsonb LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
  SELECT event_envelope FROM event_store.events
  WHERE namespace=$1 AND aggregate_type=$2 AND aggregate_id=$3 AND stream_revision >= $4
  ORDER BY stream_revision
$$;

CREATE OR REPLACE FUNCTION event_store.load_aggregate_v1(
  p_namespace text, p_aggregate_type text, p_aggregate_id uuid, p_reducer_version text
) RETURNS TABLE(frame_kind text, frame jsonb)
LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
DECLARE v_head bigint; v_snapshot_revision bigint := 0;
BEGIN
  SELECT current_revision INTO v_head FROM event_store.streams
  WHERE namespace=p_namespace AND aggregate_type=p_aggregate_type AND aggregate_id=p_aggregate_id;
  IF v_head IS NULL THEN RAISE EXCEPTION 'stream not found' USING ERRCODE = 'P0002'; END IF;
  frame_kind := 'info'; frame := jsonb_build_object('headRevision', v_head::text); RETURN NEXT;
  SELECT snapshot_revision INTO v_snapshot_revision FROM event_store.snapshots
  WHERE namespace=p_namespace AND aggregate_type=p_aggregate_type AND aggregate_id=p_aggregate_id
    AND reducer_version=p_reducer_version AND snapshot_revision <= v_head
  ORDER BY snapshot_revision DESC LIMIT 1;
  IF FOUND THEN
    frame_kind := 'snapshot';
    SELECT jsonb_build_object('snapshotRevision',snapshot_revision::text,'reducerVersion',reducer_version,'stateSchemaVersion',state_schema_version,'state',state,'stateSha256',encode(state_sha256,'hex'))
    INTO frame FROM event_store.snapshots
    WHERE namespace=p_namespace AND aggregate_type=p_aggregate_type AND aggregate_id=p_aggregate_id AND snapshot_revision=v_snapshot_revision AND reducer_version=p_reducer_version;
    RETURN NEXT;
  END IF;
  FOR frame IN SELECT event_envelope FROM event_store.events
    WHERE namespace=p_namespace AND aggregate_type=p_aggregate_type AND aggregate_id=p_aggregate_id AND stream_revision > v_snapshot_revision AND stream_revision <= v_head
    ORDER BY stream_revision
  LOOP frame_kind := 'event'; RETURN NEXT; END LOOP;
END $$;
