CREATE OR REPLACE FUNCTION event_store.set_cdc_delivery_health(p_healthy boolean)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, event_store AS $$
BEGIN
  IF p_healthy THEN RAISE EXCEPTION 'delivery health may only reopen with a timeline-bound proof' USING ERRCODE='42501'; END IF;
  UPDATE event_store.runtime_config
     SET cdc_reconciliation_required=true,
         cdc_delivery_incident_epoch=cdc_delivery_incident_epoch + CASE
           WHEN NOT cdc_reconciliation_required THEN 1 ELSE 0 END,
         cdc_delivery_healthy=false,cdc_delivery_startup_pending=false
   WHERE singleton;
END $$;
