CREATE PUBLICATION event_store_events FOR TABLE event_store.events WITH (publish = 'insert');
