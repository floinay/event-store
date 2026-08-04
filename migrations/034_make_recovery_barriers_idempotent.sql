CREATE OR REPLACE FUNCTION event_store.append_recovery_barrier(
  p_replay_id text,
  p_partition integer,
  p_aggregate_id uuid,
  p_request_id uuid
) RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_response jsonb;
BEGIN
  IF p_replay_id !~ '^[a-z0-9-]{1,63}$'
     OR p_partition NOT BETWEEN 0 AND 23
  THEN
    RAISE EXCEPTION 'invalid recovery barrier' USING ERRCODE = '22023';
  END IF;
  SELECT response INTO v_response
    FROM event_store.append_requests
   WHERE producer_service='replay-coordinator' AND request_id=p_request_id;
  IF FOUND THEN RETURN v_response; END IF;
  RETURN event_store.append_v1_without_admission(
    'replay-coordinator', 'system', 'Barrier', p_aggregate_id, p_request_id,
    'no_stream', NULL,
    jsonb_build_array(jsonb_build_object(
      'eventName', 'system.replaybarrier', 'schemaVersion', 1,
      'occurredAt', to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
      'payload', jsonb_build_object('replayId', p_replay_id, 'partition', p_partition::text)
    )),
    jsonb_build_object(
      'requestId', p_request_id::text, 'correlationId', p_request_id::text,
      'causationId', NULL,
      'actor', jsonb_build_object('kind', 'system', 'subjectRef', 'replay-coordinator')
    )
  );
END $$;

CREATE OR REPLACE FUNCTION event_store.verify_recovery_cdc_cutover(
  p_slot_name text, p_connector_name text, p_projection_name text,
  p_generation_id uuid, p_replay_id text, p_consumer_group text
) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, event_store, projection_runtime
AS $$
DECLARE v_barriers integer; v_partitions integer; v_missing integer; v_failures integer;
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
  INSERT INTO event_store.recovery_cdc_verifications(slot_name,connector_name,projection_name,generation_id,replay_id,consumer_group,kafka_lag,verified_at)
  VALUES (p_slot_name,p_connector_name,p_projection_name,p_generation_id,p_replay_id,p_consumer_group,0,clock_timestamp())
  ON CONFLICT (slot_name) DO UPDATE SET connector_name=EXCLUDED.connector_name,
    projection_name=EXCLUDED.projection_name,generation_id=EXCLUDED.generation_id,
    replay_id=EXCLUDED.replay_id,consumer_group=EXCLUDED.consumer_group,
    kafka_lag=EXCLUDED.kafka_lag,verified_at=EXCLUDED.verified_at;
END $$;
