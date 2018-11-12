# Managing Subscriptions

- Subscriptions are initiated from clients.
- Each subscription contains a topic and a geofence.
- Only one subscription per a topic for each client can exist
- If a client already subscribed to a topic, a new subscription to the same topic will overwrite the geofence

## Subscribe

Clients subscribe to topics while providing a geofence, possible cases:
1. The client is not subscribed yet
2. The client is already subscribed to the topic, thus, he wishes to update its location

### Steps Case 1.
- Create a new subscription and store it for the client
- Traverse the TopicEntryTree and find/create the fitting TopicEntry
- Add SubscriptionId to the RasterEntry, create Raster/RasterEntry if not existent
- Acknowledge (ReasonCode.Success)

### Steps Case 2.
- Calculate which RasterEntries are affected by geofence Update and add/remove necessary SubscriptionIds (This requires a COMPLEMENT operations, which seems not to be available for
spatial4j shapes. As a work around, one could also unsubscribe first)
- Update geofence of existing client subscription
- Acknowledge (ReasonCode.Success)

## Unsubscribe

Clients unsubscribe from topics, which also removes the related geofence, possible cases:
1. The client is not subscribed
2. The client is subscribed

### Steps Case 1.
- Acknowledge (ReasonCode.Success)

### Steps Case 2.
- Remove SubscriptionIds from all related RasterEntries
- Remove subscription from client
- Acknowledge (ReasonCode.Success)
