# Production topology

This overlay requires CloudNativePG 1.27+ and Strimzi 0.46+. It deploys three
PostgreSQL instances with one synchronous replica, synchronized logical slots,
three dual-role KRaft Kafka nodes, three Connect workers, and three stateless
Event Store replicas.

Apply the HA overlay, wait for the PostgreSQL primary, then provision roles and
run CDC bootstrap. The role Job owns the database only after CNPG has created
`event_store`:

```sh
kubectl apply -k deploy/production
kubectl wait --for=condition=Ready cluster/event-store-postgres --timeout=10m
kubectl apply -f deploy/event-store/cluster-roles-job.yaml
kubectl wait --for=condition=complete job/event-store-cluster-roles --timeout=5m
kubectl apply -f deploy/event-store/bootstrap-job.yaml
```

`recorded_at` is the authoritative UTC event time. `recorded_at_kafka` is a
derived UTC `timestamp without time zone` used only by Debezium's EventRouter.
The default adaptive Debezium timestamp mapping supplies the Kafka record
timestamp; `time.precision.mode=connect` is incompatible with EventRouter 3.6.
`event_envelope_kafka` holds the canonical JSON text derived in the same append
transaction. With `StringConverter` and `expand.json.payload=false`, Debezium
publishes those canonical JSON bytes without reserializing the JSONB envelope.
The CDC latency probe starts immediately after the Event Store's successful SQL
call, before consumer validation or handlers. `recorded_at` is domain time and
is not used as the commit timestamp. Production clock skew between measured
nodes must remain within 2 ms.

Before a planned promotion, run
`SELECT event_store.assert_configured_failover_candidate()` on the selected
candidate. A synchronized slot lets Debezium resume without recovery. CNPG may
automatically promote after an unplanned failure; then append admission must
remain fail-closed until the configured slot, Connect, and Kafka delivery are
healthy. If the slot is absent, invalid, or cannot resume from its durable
offset, run slot-loss recovery and reconcile all `event_id` values before
reopening append traffic.

Delivery health is bound to PostgreSQL's promotion timeline. A promoted
standby inherits the old state but cannot append until a replica verifies the
slot, Connect, and Kafka again on the new timeline.

Each Event Store replica checks that delivery chain every five seconds
(`CDC_DELIVERY_HEALTH_CHECK_INTERVAL_MS`). A failed check persistently closes
append admission in PostgreSQL, including for existing gRPC connections; a
healthy chain reopens it. When the latency probe is configured, its fresh
read-committed consumer receipt is also required before reopening. Storage-only
deployments keep CDC admission disabled and do not use this fence.
