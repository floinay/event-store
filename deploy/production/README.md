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

Before promoting a PostgreSQL standby, run
`SELECT event_store.assert_configured_failover_candidate()` on that
candidate. Promotion is prohibited unless the slot is present, failover-enabled,
non-temporary, valid, and `synced=true`. After promotion, redirect the primary
Service and Connect, verify the slot identity/LSN, reconcile event IDs around
the promotion window, then reopen append traffic.
