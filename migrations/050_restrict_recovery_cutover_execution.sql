REVOKE ALL ON FUNCTION event_store.activate_recovery_cdc_slot(text,text,bigint)
  FROM PUBLIC;
GRANT EXECUTE ON FUNCTION event_store.activate_recovery_cdc_slot(text,text,bigint)
  TO event_store_cdc;
