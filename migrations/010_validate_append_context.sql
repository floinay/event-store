ALTER FUNCTION event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)
  RENAME TO append_v1_unchecked;

REVOKE ALL ON FUNCTION event_store.append_v1_unchecked(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)
  FROM PUBLIC, event_store_app;

CREATE FUNCTION event_store.append_v1(
  p_producer_service text, p_namespace text, p_aggregate_type text, p_aggregate_id uuid,
  p_request_id uuid, p_expected_kind text, p_expected_revision bigint, p_events jsonb, p_context jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
BEGIN
  IF jsonb_typeof(p_context) <> 'object'
    OR coalesce(p_context->>'correlationId' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', false) = false
    OR (p_context ? 'causationId' AND p_context->>'causationId' IS NOT NULL
      AND coalesce(p_context->>'causationId' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', false) = false)
    OR coalesce(p_context->'actor'->>'kind' IN ('user', 'service', 'system'), false) = false
    OR coalesce(p_context->'actor'->>'subjectRef', '') = ''
  THEN
    RAISE EXCEPTION 'invalid event context' USING ERRCODE = '22023';
  END IF;
  RETURN event_store.append_v1_unchecked(
    p_producer_service, p_namespace, p_aggregate_type, p_aggregate_id,
    p_request_id, p_expected_kind, p_expected_revision, p_events, p_context
  );
END $$;

GRANT EXECUTE ON FUNCTION event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)
  TO event_store_app;
