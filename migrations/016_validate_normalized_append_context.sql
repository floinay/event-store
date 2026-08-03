CREATE OR REPLACE FUNCTION event_store.append_v1(
  p_producer_service text, p_namespace text, p_aggregate_type text, p_aggregate_id uuid,
  p_request_id uuid, p_expected_kind text, p_expected_revision bigint, p_events jsonb, p_context jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
DECLARE v_context jsonb;
BEGIN
  v_context := CASE
    WHEN jsonb_typeof(p_context) = 'object' AND NOT (p_context ? 'causationId')
      THEN p_context || jsonb_build_object('causationId', null)
    ELSE p_context
  END;
  IF jsonb_typeof(v_context) <> 'object'
    OR coalesce(v_context->>'correlationId' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', false) = false
    OR (v_context ? 'causationId' AND v_context->>'causationId' IS NOT NULL
      AND coalesce(v_context->>'causationId' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', false) = false)
    OR coalesce(v_context->'actor'->>'kind' IN ('user', 'service', 'system'), false) = false
    OR coalesce(v_context->'actor'->>'subjectRef', '') = ''
  THEN
    RAISE EXCEPTION 'invalid event context' USING ERRCODE = '22023';
  END IF;
  RETURN event_store.append_v1_unchecked(
    p_producer_service, p_namespace, p_aggregate_type, p_aggregate_id,
    p_request_id, p_expected_kind, p_expected_revision, p_events, v_context
  );
END $$;
