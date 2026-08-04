CREATE FUNCTION event_store.contains_direct_pii(p_value jsonb)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
STRICT
SET search_path = pg_catalog, event_store
AS $$
  SELECT CASE jsonb_typeof($1)
    WHEN 'object' THEN EXISTS (
      SELECT 1 FROM jsonb_each($1) AS entries(key, value)
      WHERE (
               key !~* '^(eventName|schemaVersion|occurredAt|requestId|correlationId|causationId|aggregateId|aggregateType|subjectRef)$'
           AND key ~* '(^|[_-])(name|first[_-]?name|last[_-]?name|email|e[_-]?mail|phone|telephone|address|token|password|credential|secret|card([_-]?number)?|pan|cvv|ssn|social[_-]?security|date[_-]?of[_-]?birth|dob)$|(email|phone|address|token|password|credential|secret|cardnumber|pan|cvv|ssn|dob)$'
            )
         OR event_store.contains_direct_pii(value)
    )
    WHEN 'array' THEN EXISTS (
      SELECT 1 FROM jsonb_array_elements($1) AS entries(value)
      WHERE event_store.contains_direct_pii(value)
    )
    ELSE false
  END
$$;

CREATE FUNCTION event_store.contains_unrepresentable_json_number(p_value jsonb)
RETURNS boolean
LANGUAGE plpgsql
IMMUTABLE
STRICT
SET search_path = pg_catalog, event_store
AS $$
DECLARE v_value jsonb; v_text text; v_double double precision;
BEGIN
  CASE jsonb_typeof(p_value)
    WHEN 'number' THEN
      v_text := p_value #>> '{}';
      BEGIN
        v_double := v_text::double precision;
      EXCEPTION WHEN OTHERS THEN
        RETURN true;
      END;
      IF v_double = 'Infinity'::double precision
         OR v_double = '-Infinity'::double precision
         OR v_double <> v_double THEN
        RETURN true;
      END IF;
      RETURN v_text::numeric <> v_double::numeric;
    WHEN 'array' THEN
      FOR v_value IN SELECT value FROM jsonb_array_elements(p_value) AS entries(value) LOOP
        IF event_store.contains_unrepresentable_json_number(v_value) THEN RETURN true; END IF;
      END LOOP;
    WHEN 'object' THEN
      FOR v_value IN SELECT value FROM jsonb_each(p_value) AS entries(key, value) LOOP
        IF event_store.contains_unrepresentable_json_number(v_value) THEN RETURN true; END IF;
      END LOOP;
    ELSE
      NULL;
  END CASE;
  RETURN false;
END $$;

ALTER FUNCTION event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)
  RENAME TO append_v1_admitted;
REVOKE ALL ON FUNCTION event_store.append_v1_admitted(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)
  FROM PUBLIC, event_store_app, event_store_cdc, projection_worker;

CREATE FUNCTION event_store.append_v1(
  p_producer_service text, p_namespace text, p_aggregate_type text, p_aggregate_id uuid,
  p_request_id uuid, p_expected_kind text, p_expected_revision bigint, p_events jsonb, p_context jsonb
) RETURNS jsonb
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
BEGIN
  IF event_store.contains_direct_pii(p_events) OR event_store.contains_direct_pii(p_context) THEN
    RAISE EXCEPTION 'direct PII field is prohibited' USING ERRCODE = '22023';
  END IF;
  IF event_store.contains_unrepresentable_json_number(p_events) OR event_store.contains_unrepresentable_json_number(p_context) THEN
    RAISE EXCEPTION 'JSON number is not representable by canonical JavaScript consumer' USING ERRCODE = '22023';
  END IF;
  RETURN event_store.append_v1_admitted(
    p_producer_service, p_namespace, p_aggregate_type, p_aggregate_id,
    p_request_id, p_expected_kind, p_expected_revision, p_events, p_context
  );
END $$;
REVOKE ALL ON FUNCTION event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb) TO event_store_app;
