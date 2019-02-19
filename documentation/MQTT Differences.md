# Differences to MQTT v5

- We need a NotConnected Reason code as tcp connections are managed by ZeroMQ

- CONNECT creates client, DISCONNECT deletes it

- PINGREQ contains the client's location
- PINGRESP contains NotConnected when not connected and UpdatedLocation when location was updated

- SUBSCRIBE: only subscribes to one topic at once, does not support shared or QoS0/2, we do not use subscription identifiers, needs to contain topic and geofence
- SUBACK: contains NotConnected when not connected

- If message misses mandatory fields, the server discards it
- QoS1 only valid until ZeroMQ discards messages


TODOs:
- add UNSUBSCRIBE
- Add keepalive time in connect
