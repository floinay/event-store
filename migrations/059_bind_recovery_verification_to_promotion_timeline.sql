-- A recovery proof belongs to the PostgreSQL history in which it was made.
-- Existing proof rows deliberately remain unbound, forcing a fresh proof after
-- this migration or any subsequent promotion.
ALTER TABLE event_store.recovery_cdc_verifications
  ADD COLUMN verified_timeline_id integer;

CREATE OR REPLACE FUNCTION event_store.verify_recovery_cdc_cutover(
  p_slot_name text, p_connector_name text, p_projection_name text,
  p_generation_id uuid, p_replay_id text, p_consumer_group text
) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, event_store, projection_runtime
AS $$
DECLARE v_barriers integer; v_partitions integer; v_missing integer; v_failures integer; v_timeline_id integer;
BEGIN
  IF p_slot_name !~ '^event_store_[a-z0-9_]{1,50}$'
     OR p_connector_name !~ '^event-store-[a-z0-9-]{1,63}$'
     OR p_replay_id !~ '^[a-z0-9-]{1,63}$' OR p_consumer_group = ''
  THEN RAISE EXCEPTION 'invalid recovery cutover verification' USING ERRCODE='22023'; END IF;
  SELECT count(*)::int, count(DISTINCT i.partition_no)::int
    INTO v_barriers, v_partitions
    FROM event_store.events e
    JOIN projection_runtime.inbox i ON i.event_id=e.event_id
    JOIN projection_runtime.checkpoints c ON c.projection_name=i.projection_name
      AND c.generation_id=i.generation_id AND c.topic_name=i.topic_name AND c.partition_no=i.partition_no
   WHERE i.projection_name=p_projection_name AND i.generation_id=p_generation_id
     AND e.event_name='system.replaybarrier'
     AND e.event_envelope->'payload'->>'replayId'=p_replay_id
     AND c.next_offset > i.kafka_offset;
  IF v_barriers <> 24 OR v_partitions <> 24 THEN
    RAISE EXCEPTION 'one durable recovery barrier is required in each Kafka partition' USING ERRCODE='P0001';
  END IF;
  SELECT count(*)::int INTO v_missing FROM event_store.events e WHERE NOT EXISTS (
    SELECT 1 FROM projection_runtime.inbox i WHERE i.projection_name=p_projection_name
      AND i.generation_id=p_generation_id AND i.event_id=e.event_id);
  IF v_missing <> 0 THEN RAISE EXCEPTION 'recovery event-id reconciliation is incomplete' USING ERRCODE='P0001'; END IF;
  SELECT count(*)::int INTO v_failures FROM projection_runtime.failures
   WHERE projection_name=p_projection_name AND generation_id=p_generation_id;
  IF v_failures <> 0 THEN RAISE EXCEPTION 'recovery projection has failures' USING ERRCODE='P0001'; END IF;
  v_timeline_id := event_store.current_timeline_id();
  INSERT INTO event_store.recovery_cdc_verifications(
    slot_name,connector_name,projection_name,generation_id,replay_id,consumer_group,
    kafka_lag,verified_at,verified_timeline_id
  ) VALUES (
    p_slot_name,p_connector_name,p_projection_name,p_generation_id,p_replay_id,p_consumer_group,
    0,clock_timestamp(),v_timeline_id
  ) ON CONFLICT (slot_name) DO UPDATE SET connector_name=EXCLUDED.connector_name,
    projection_name=EXCLUDED.projection_name,generation_id=EXCLUDED.generation_id,
    replay_id=EXCLUDED.replay_id,consumer_group=EXCLUDED.consumer_group,
    kafka_lag=EXCLUDED.kafka_lag,verified_at=EXCLUDED.verified_at,
    verified_timeline_id=EXCLUDED.verified_timeline_id;
END $$;

GRANT EXECUTE ON FUNCTION event_store.current_timeline_id()
  TO event_store_app, event_store_cdc;

CREATE OR REPLACE FUNCTION event_store.activate_recovery_cdc_slot(
  p_slot_name text,
  p_connector_name text,
  p_wal_budget_bytes bigint
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_verified boolean; v_timeline_id integer;
BEGIN
  IF p_slot_name !~ '^event_store_[a-z0-9_]{1,50}$'
     OR p_connector_name !~ '^event-store-[a-z0-9-]{1,63}$' THEN
    RAISE EXCEPTION 'invalid recovery CDC identity' USING ERRCODE = '22023';
  END IF;
  IF p_wal_budget_bytes <= 0 THEN
    RAISE EXCEPTION 'WAL budget must be positive' USING ERRCODE = '22023';
  END IF;
  PERFORM event_store.assert_recovery_cdc_slot_ready(p_slot_name);
  v_timeline_id := event_store.current_timeline_id();
  SELECT true INTO v_verified
    FROM event_store.recovery_cdc_verifications
   WHERE slot_name=p_slot_name
     AND connector_name=p_connector_name
     AND kafka_lag=0
     AND verified_timeline_id=v_timeline_id;
  IF v_verified IS DISTINCT FROM true THEN
    RAISE EXCEPTION 'recovery CDC cutover has not been verified on the current timeline' USING ERRCODE = 'P0001';
  END IF;
  UPDATE event_store.runtime_config
     SET append_admission_enabled=true,
         cdc_delivery_healthy=true,
         cdc_delivery_timeline_id=v_timeline_id,
         wal_budget_bytes=p_wal_budget_bytes,
         cdc_slot_name=p_slot_name,
         cdc_connector_name=p_connector_name
   WHERE singleton;
END $$;
