CREATE TABLE projection_runtime.generations (
  projection_name text NOT NULL, generation_id uuid NOT NULL, status text NOT NULL,
  created_at timestamptz NOT NULL, activated_at timestamptz,
  PRIMARY KEY (projection_name, generation_id), CHECK (status IN ('building', 'active', 'retired'))
);
CREATE UNIQUE INDEX one_active_projection_generation ON projection_runtime.generations (projection_name) WHERE status = 'active';

CREATE TABLE projection_runtime.checkpoints (
  projection_name text NOT NULL, generation_id uuid NOT NULL, topic_name text NOT NULL, partition_no integer NOT NULL,
  next_offset bigint NOT NULL, last_event_id uuid, updated_at timestamptz NOT NULL,
  PRIMARY KEY (projection_name, generation_id, topic_name, partition_no),
  FOREIGN KEY (projection_name, generation_id) REFERENCES projection_runtime.generations,
  CHECK (partition_no >= 0), CHECK (next_offset >= 0)
);
CREATE TABLE projection_runtime.inbox (
  projection_name text NOT NULL, generation_id uuid NOT NULL, event_id uuid NOT NULL, envelope_sha256 char(64) NOT NULL,
  topic_name text NOT NULL, partition_no integer NOT NULL, kafka_offset bigint NOT NULL, processed_at timestamptz NOT NULL,
  PRIMARY KEY (projection_name, generation_id, event_id),
  UNIQUE (projection_name, generation_id, topic_name, partition_no, kafka_offset),
  CHECK (envelope_sha256 ~ '^[0-9a-f]{64}$')
);
CREATE TABLE projection_runtime.failures (
  projection_name text NOT NULL, generation_id uuid NOT NULL, event_id uuid NOT NULL, envelope_sha256 char(64) NOT NULL,
  topic_name text NOT NULL, partition_no integer NOT NULL, kafka_offset bigint NOT NULL, attempt_count integer NOT NULL,
  error_code text NOT NULL, error_detail jsonb NOT NULL, envelope jsonb NOT NULL,
  first_failed_at timestamptz NOT NULL, last_failed_at timestamptz NOT NULL, dlq_published_at timestamptz,
  PRIMARY KEY (projection_name, generation_id, topic_name, partition_no, kafka_offset), CHECK (attempt_count > 0)
);
