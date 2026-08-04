# Replay workflow

`REPLAY_ACTION=start` creates the temporary connector, the building generation,
and one partition-verified barrier per replay partition. It prints the barrier
event IDs.

The replay projection consumer must call `ReplayCoordinator.recordBarrier()`
when it processes each barrier. Once all barriers are committed, run
`REPLAY_ACTION=activate` with that consumer group ID, its runtime-measured
readable-record lag, and the expected and rebuilt-model checksums. The command
rejects incomplete barriers, lag, failures, or checksum mismatch; on success it
atomically activates the generation and deletes the temporary connector and
logical slot.
