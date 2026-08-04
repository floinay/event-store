# Replay workflow

`REPLAY_ACTION=start` creates the temporary connector, the building generation,
and one partition-verified barrier per replay partition. It prints the barrier
event IDs.

The replay projection consumer must call `ReplayCoordinator.recordBarrier()`
when it processes each barrier. For activation it must provide a module through
`REPLAY_VERIFICATION_MODULE`; that module exports
`createReplayVerification(identity)`, returning the consumer's group ID plus
functions which measure its readable-record lag and calculate the full-fold and
rebuilt-model checksums. Shell-provided lag or checksum strings are rejected.
The command rejects incomplete barriers, lag, failures, or checksum mismatch;
on success it atomically activates the generation and deletes the temporary
connector and logical slot.
