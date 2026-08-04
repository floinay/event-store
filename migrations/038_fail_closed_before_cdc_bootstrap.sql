ALTER TABLE event_store.runtime_config
  ALTER COLUMN append_admission_enabled SET DEFAULT true;

UPDATE event_store.runtime_config
  SET append_admission_enabled=true
  WHERE singleton;
