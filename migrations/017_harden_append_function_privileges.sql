REVOKE ALL ON FUNCTION event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)
  FROM PUBLIC;
REVOKE ALL ON FUNCTION event_store.append_v1_unchecked(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)
  FROM PUBLIC, event_store_app, event_store_cdc, projection_worker;
GRANT EXECUTE ON FUNCTION event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb)
  TO event_store_app;
