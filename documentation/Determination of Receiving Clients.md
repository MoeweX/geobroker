# Determination of Receiving Clients

When a published message is received, the following steps have to be performed:

Find interested subscribers (done via TopicAndGeofenceMapper):
- Who has subscribed to the topic
- Who has a geofence in which the publisher is located

Check whether the subscriber qualified (done via TODO)?
- We already new, that the subscriber has interest in topic
- Is the subscribers located inside the geofence of the published message
