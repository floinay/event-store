CREATE TABLE projection_runtime.replay_barriers (
  projection_name text NOT NULL,
  generation_id uuid NOT NULL,
  partition_no integer NOT NULL CHECK (partition_no BETWEEN 0 AND 23),
  event_id uuid NOT NULL,
  processed_at timestamptz NOT NULL,
  PRIMARY KEY (projection_name, generation_id, partition_no),
  UNIQUE (projection_name, generation_id, event_id),
  FOREIGN KEY (projection_name, generation_id)
    REFERENCES projection_runtime.generations
);
