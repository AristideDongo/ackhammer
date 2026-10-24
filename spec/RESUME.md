# Resume Contract (v0.3)

- Cursor opaque: `type Cursor []byte`
- Checkpoint after business effect or in same transaction
- SSE: Last-Event-ID, WebSocket: resume message, MQTT: persistent session + QoS1
- CloudEvent with id, source, sequence, partitionkey, traceparent
