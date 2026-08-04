CREATE TABLE event_store.recovery_cdc_verifications (
  slot_name text PRIMARY KEY,
  connector_name text NOT NULL,
  projection_name text NOT NULL,
  generation_id uuid NOT NULL,
  replay_id text NOT NULL,
  consumer_group text NOT NULL,
  kafka_lag bigint NOT NULL CHECK (kafka_lag = 0),
  verified_at timestamptz NOT NULL,
  FOREIGN KEY (projection_name, generation_id)
    REFERENCES projection_runtime.generations(projection_name, generation_id)
);

CREATE FUNCTION event_store.verify_recovery_cdc_cutover(
  p_slot_name text,
  p_connector_name text,
  p_projection_name text,
  p_generation_id uuid,
  p_replay_id text,
  p_consumer_group text,
  p_kafka_lag bigint
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store, projection_runtime
AS $$
DECLARE v_barriers integer; v_missing integer; v_failures integer;
BEGIN
  IF p_slot_name !~ '^event_store_[a-z0-9_]{1,50}$'
     OR p_connector_name !~ '^event-store-[a-z0-9-]{1,63}$'
     OR p_replay_id !~ '^[a-z0-9-]{1,63}$'
     OR p_consumer_group = ''
     OR p_kafka_lag <> 0
  THEN
    RAISE EXCEPTION 'invalid recovery cutover verification' USING ERRCODE = '22023';
  END IF;
  SELECT count(*)::int INTO v_barriers
    FROM event_store.events e
    JOIN projection_runtime.inbox i ON i.event_id=e.event_id
   WHERE i.projection_name=p_projection_name
     AND i.generation_id=p_generation_id
     AND e.event_name='system.replaybarrier'
     AND e.event_envelope->'payload'->>'replayId'=p_replay_id;
  IF v_barriers <> 24 THEN
    RAISE EXCEPTION 'all 24 recovery barriers must be durably processed'
      USING ERRCODE = 'P0001';
  END IF;
  SELECT count(*)::int INTO v_missing
    FROM event_store.events e
   WHERE NOT EXISTS (
     SELECT 1 FROM projection_runtime.inbox i
      WHERE i.projection_name=p_projection_name
        AND i.generation_id=p_generation_id
        AND i.event_id=e.event_id
   );
  IF v_missing <> 0 THEN
    RAISE EXCEPTION 'recovery event-id reconciliation is incomplete'
      USING ERRCODE = 'P0001';
  END IF;
  SELECT count(*)::int INTO v_failures
    FROM projection_runtime.failures
   WHERE projection_name=p_projection_name AND generation_id=p_generation_id;
  IF v_failures <> 0 THEN
    RAISE EXCEPTION 'recovery projection has failures' USING ERRCODE = 'P0001';
  END IF;
  INSERT INTO event_store.recovery_cdc_verifications(
    slot_name,connector_name,projection_name,generation_id,replay_id,
    consumer_group,kafka_lag,verified_at
  ) VALUES (
    p_slot_name,p_connector_name,p_projection_name,p_generation_id,p_replay_id,
    p_consumer_group,p_kafka_lag,clock_timestamp()
  ) ON CONFLICT (slot_name) DO UPDATE SET
    connector_name=EXCLUDED.connector_name,
    projection_name=EXCLUDED.projection_name,
    generation_id=EXCLUDED.generation_id,
    replay_id=EXCLUDED.replay_id,
    consumer_group=EXCLUDED.consumer_group,
    kafka_lag=EXCLUDED.kafka_lag,
    verified_at=EXCLUDED.verified_at;
END $$;

CREATE OR REPLACE FUNCTION event_store.activate_recovery_cdc_slot(
  p_slot_name text,
  p_connector_name text,
  p_wal_budget_bytes bigint
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_slot record; v_verified boolean;
BEGIN
  IF p_slot_name !~ '^event_store_[a-z0-9_]{1,50}$'
     OR p_connector_name !~ '^event-store-[a-z0-9-]{1,63}$' THEN
    RAISE EXCEPTION 'invalid recovery CDC identity' USING ERRCODE = '22023';
  END IF;
  IF p_wal_budget_bytes <= 0 THEN
    RAISE EXCEPTION 'WAL budget must be positive' USING ERRCODE = '22023';
  END IF;
  SELECT active, invalidation_reason, restart_lsn INTO v_slot
    FROM pg_replication_slots WHERE slot_name=p_slot_name;
  IF NOT FOUND OR NOT v_slot.active OR v_slot.invalidation_reason IS NOT NULL OR v_slot.restart_lsn IS NULL THEN
    RAISE EXCEPTION 'recovery CDC slot is not ready' USING ERRCODE = 'P0001';
  END IF;
  SELECT true INTO v_verified
    FROM event_store.recovery_cdc_verifications
   WHERE slot_name=p_slot_name
     AND connector_name=p_connector_name
     AND kafka_lag=0;
  IF v_verified IS DISTINCT FROM true THEN
    RAISE EXCEPTION 'recovery CDC cutover has not been verified' USING ERRCODE = 'P0001';
  END IF;
  UPDATE event_store.runtime_config
    SET append_admission_enabled=true,
        wal_budget_bytes=p_wal_budget_bytes,
        cdc_slot_name=p_slot_name,
        cdc_connector_name=p_connector_name
    WHERE singleton;
END $$;

REVOKE ALL ON FUNCTION event_store.verify_recovery_cdc_cutover(text,text,text,uuid,text,text,bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.verify_recovery_cdc_cutover(text,text,text,uuid,text,text,bigint) TO event_store_cdc;
