-- A promoted standby inherits runtime_config, including a previously healthy
-- delivery flag.  PostgreSQL promotion creates a new timeline, so bind that
-- flag to the verified timeline and reject every append until this primary has
-- completed a fresh delivery-chain check.
ALTER TABLE event_store.runtime_config
  ADD COLUMN cdc_delivery_timeline_id integer;

CREATE FUNCTION event_store.current_timeline_id()
RETURNS integer
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  SELECT (pg_control_checkpoint()).timeline_id
$$;

CREATE FUNCTION event_store.assert_cdc_delivery_timeline(p_timeline_id integer)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
BEGIN
  IF p_timeline_id IS DISTINCT FROM event_store.current_timeline_id() THEN
    RAISE EXCEPTION 'CDC delivery has not been verified on this promotion timeline'
      USING ERRCODE='P0001';
  END IF;
END $$;

CREATE OR REPLACE FUNCTION event_store.set_cdc_delivery_health(p_healthy boolean)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_wal_budget_bytes bigint; v_timeline_id integer;
BEGIN
  IF p_healthy THEN
    SELECT wal_budget_bytes INTO v_wal_budget_bytes
      FROM event_store.runtime_config WHERE singleton;
    PERFORM event_store.assert_cdc_delivery_ready(v_wal_budget_bytes);
    v_timeline_id := event_store.current_timeline_id();
  END IF;
  UPDATE event_store.runtime_config
     SET cdc_delivery_healthy=p_healthy,
         cdc_delivery_timeline_id=CASE WHEN p_healthy THEN v_timeline_id ELSE cdc_delivery_timeline_id END
   WHERE singleton;
END $$;

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
         cdc_bootstrap_complete=true,
         cdc_delivery_healthy=true,
         cdc_delivery_timeline_id=event_store.current_timeline_id(),
         wal_budget_bytes=p_wal_budget_bytes
   WHERE singleton;
END $$;

CREATE OR REPLACE FUNCTION event_store.append_v1(
  p_producer_service text, p_namespace text, p_aggregate_type text, p_aggregate_id uuid,
  p_request_id uuid, p_expected_kind text, p_expected_revision bigint, p_events jsonb, p_context jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
DECLARE v_config event_store.runtime_config%ROWTYPE;
BEGIN
  IF event_store.contains_direct_pii(p_events) OR event_store.contains_direct_pii(p_context) THEN
    RAISE EXCEPTION 'direct PII field is prohibited' USING ERRCODE = '22023';
  END IF;
  IF event_store.contains_unrepresentable_json_number(p_events)
     OR event_store.contains_unrepresentable_json_number(p_context) THEN
    RAISE EXCEPTION 'JSON number is not representable by canonical JavaScript consumer' USING ERRCODE = '22023';
  END IF;
  SELECT * INTO v_config FROM event_store.runtime_config WHERE singleton;
  IF v_config.append_admission_enabled THEN
    IF v_config.cdc_delivery_healthy IS DISTINCT FROM true THEN
      RAISE EXCEPTION 'CDC delivery health has closed append admission' USING ERRCODE='P0001';
    END IF;
    PERFORM event_store.assert_cdc_delivery_timeline(v_config.cdc_delivery_timeline_id);
    PERFORM event_store.assert_append_cdc_ready(v_config.wal_budget_bytes, false);
  END IF;
  RETURN event_store.append_v1_validated(
    p_producer_service, p_namespace, p_aggregate_type, p_aggregate_id,
    p_request_id, p_expected_kind, p_expected_revision, p_events, p_context
  );
END $$;

CREATE OR REPLACE FUNCTION event_store.append_v1_critical(
  p_producer_service text, p_namespace text, p_aggregate_type text, p_aggregate_id uuid,
  p_request_id uuid, p_expected_kind text, p_expected_revision bigint, p_events jsonb, p_context jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
DECLARE v_config event_store.runtime_config%ROWTYPE;
BEGIN
  IF session_user <> 'event_store_critical_app' THEN
    RAISE EXCEPTION 'critical append principal is required' USING ERRCODE='42501';
  END IF;
  IF event_store.contains_direct_pii(p_events) OR event_store.contains_direct_pii(p_context) THEN
    RAISE EXCEPTION 'direct PII field is prohibited' USING ERRCODE = '22023';
  END IF;
  IF event_store.contains_unrepresentable_json_number(p_events)
     OR event_store.contains_unrepresentable_json_number(p_context) THEN
    RAISE EXCEPTION 'JSON number is not representable by canonical JavaScript consumer' USING ERRCODE = '22023';
  END IF;
  SELECT * INTO v_config FROM event_store.runtime_config WHERE singleton;
  IF v_config.append_admission_enabled THEN
    IF v_config.cdc_delivery_healthy IS DISTINCT FROM true THEN
      RAISE EXCEPTION 'CDC delivery health has closed append admission' USING ERRCODE='P0001';
    END IF;
    PERFORM event_store.assert_cdc_delivery_timeline(v_config.cdc_delivery_timeline_id);
    PERFORM event_store.assert_append_cdc_ready(v_config.wal_budget_bytes, true);
  END IF;
  RETURN event_store.append_v1_validated(
    p_producer_service, p_namespace, p_aggregate_type, p_aggregate_id,
    p_request_id, p_expected_kind, p_expected_revision, p_events, p_context
  );
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
         cdc_delivery_healthy=true,
         cdc_delivery_timeline_id=event_store.current_timeline_id(),
         wal_budget_bytes=p_wal_budget_bytes,
         cdc_slot_name=p_slot_name,
         cdc_connector_name=p_connector_name
   WHERE singleton;
END $$;
