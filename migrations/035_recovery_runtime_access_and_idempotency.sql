GRANT SELECT ON event_store.runtime_config TO event_store_app, event_store_cdc;

CREATE OR REPLACE FUNCTION event_store.append_recovery_barrier(
  p_replay_id text, p_partition integer, p_aggregate_id uuid, p_request_id uuid
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_response jsonb;
BEGIN
  IF p_replay_id !~ '^[a-z0-9-]{1,63}$' OR p_partition NOT BETWEEN 0 AND 23
  THEN RAISE EXCEPTION 'invalid recovery barrier' USING ERRCODE='22023'; END IF;
  PERFORM pg_advisory_xact_lock(
    hashtextextended('replay-coordinator:' || p_request_id::text, 0)
  );
  SELECT response INTO v_response FROM event_store.append_requests
   WHERE producer_service='replay-coordinator' AND request_id=p_request_id;
  IF FOUND THEN RETURN v_response; END IF;
  RETURN event_store.append_v1_without_admission(
    'replay-coordinator','system','Barrier',p_aggregate_id,p_request_id,'no_stream',NULL,
    jsonb_build_array(jsonb_build_object(
      'eventName','system.replaybarrier','schemaVersion',1,
      'occurredAt',to_char(clock_timestamp() AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
      'payload',jsonb_build_object('replayId',p_replay_id,'partition',p_partition::text)
    )),
    jsonb_build_object('requestId',p_request_id::text,'correlationId',p_request_id::text,
      'causationId',NULL,'actor',jsonb_build_object('kind','system','subjectRef','replay-coordinator'))
  );
END $$;
