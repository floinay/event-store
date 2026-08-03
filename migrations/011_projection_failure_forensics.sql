ALTER TABLE projection_runtime.failures
  ALTER COLUMN event_id DROP NOT NULL,
  ALTER COLUMN envelope_sha256 DROP NOT NULL;
