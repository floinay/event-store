CREATE OR REPLACE FUNCTION event_store.contains_direct_pii(p_value jsonb)
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
               regexp_replace(lower(key), '[^a-z0-9]', '', 'g') NOT IN
                 ('eventname','schemaversion','occurredat','requestid','correlationid',
                  'causationid','aggregateid','aggregatetype','subjectref')
           AND regexp_replace(lower(key), '[^a-z0-9]', '', 'g') ~
                 '(name|firstname|lastname|email|phone|phonenumber|telephone|telephonenumber|address|token|password|credential|secret|cardnumber|pan|cvv|iban|bic|swift|bankaccount|accountnumber|routingnumber|ssn|socialsecuritynumber|dateofbirth|birthdate|dob)$'
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
