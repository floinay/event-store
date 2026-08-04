-- A barrier is only proof of a replay when it originated from the replay topic
-- and the projection group has committed past that exact Kafka record.
ALTER TABLE projection_runtime.replay_barriers
  ADD COLUMN topic_name text,
  ADD COLUMN kafka_offset bigint;

ALTER TABLE projection_runtime.replay_barriers
  ADD CONSTRAINT replay_barriers_kafka_offset_nonnegative
  CHECK (kafka_offset IS NULL OR kafka_offset >= 0);
