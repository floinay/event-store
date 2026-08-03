REVOKE ALL ON ALL TABLES IN SCHEMA event_store FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA event_store FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA event_store FROM PUBLIC;

GRANT CONNECT ON DATABASE event_store TO event_store_app, event_store_cdc, projection_worker;
GRANT USAGE ON SCHEMA event_store TO event_store_app;
GRANT EXECUTE ON FUNCTION event_store.append_v1(text,text,text,uuid,uuid,text,bigint,jsonb,jsonb) TO event_store_app;
GRANT EXECUTE ON FUNCTION event_store.put_snapshot_v1(text,text,uuid,bigint,text,integer,jsonb,bytea) TO event_store_app;
GRANT EXECUTE ON FUNCTION event_store.get_stream_head_v1(text,text,uuid) TO event_store_app;
GRANT EXECUTE ON FUNCTION event_store.read_stream_v1(text,text,uuid,bigint) TO event_store_app;
GRANT EXECUTE ON FUNCTION event_store.load_aggregate_v1(text,text,uuid,text) TO event_store_app;
GRANT USAGE ON SCHEMA event_store TO event_store_cdc;
GRANT SELECT ON event_store.events TO event_store_cdc;
GRANT USAGE ON SCHEMA projection_runtime TO projection_worker;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA projection_runtime TO projection_worker;
