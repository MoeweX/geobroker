package de.hasenburg.geobroker.server.matching;

import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.ReasonCode;
import de.hasenburg.geobroker.commons.model.message.Topic;
import de.hasenburg.geobroker.commons.model.message.payloads.CONNACKPayload;
import de.hasenburg.geobroker.commons.model.message.payloads.DISCONNECTPayload;
import de.hasenburg.geobroker.commons.model.message.payloads.PINGRESPPayload;
import de.hasenburg.geobroker.commons.model.message.payloads.SUBACKPayload;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import de.hasenburg.geobroker.server.communication.InternalServerMessage;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import de.hasenburg.geobroker.server.storage.client.ClientDirectory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.Logger;

class CommonMatchingTasks {

	static InternalServerMessage connectClientAtLocalBroker(String clientIdentifier, Location location,
															ClientDirectory clientDirectory, Logger logger) {

		boolean success = clientDirectory.addClient(clientIdentifier, location);

		if (success) {
			logger.debug("Created client {}, acknowledging.", clientIdentifier);
			return new InternalServerMessage(clientIdentifier,
					ControlPacketType.CONNACK,
					new CONNACKPayload(ReasonCode.Success));
		} else {
			logger.debug("Client {} already exists, so protocol error. Disconnecting.", clientIdentifier);
			clientDirectory.removeClient(clientIdentifier);
			return new InternalServerMessage(clientIdentifier,
					ControlPacketType.DISCONNECT,
					new DISCONNECTPayload(ReasonCode.ProtocolError));
		}
	}

	static InternalServerMessage updateClientLocationAtLocalBroker(String clientIdentifier, Location location,
																   ClientDirectory clientDirectory, Logger logger) {

		boolean success = clientDirectory.updateClientLocation(clientIdentifier, location);
		if (success) {
			logger.debug("Updated location of {} to {}", clientIdentifier, location);

			return new InternalServerMessage(clientIdentifier,
					ControlPacketType.PINGRESP,
					new PINGRESPPayload(ReasonCode.LocationUpdated));
		} else {
			logger.debug("Client {} is not connected", clientIdentifier);
			return new InternalServerMessage(clientIdentifier,
					ControlPacketType.PINGRESP,
					new PINGRESPPayload(ReasonCode.NotConnected));
		}

	}

	static InternalServerMessage subscribeAtLocalBroker(String clientIdentifier, ClientDirectory clientDirectory,
														TopicAndGeofenceMapper topicAndGeofenceMapper, Topic topic,
														Geofence geofence, Logger logger) {
		ImmutablePair<ImmutablePair<String, Integer>, Geofence> subscribed = clientDirectory.checkIfSubscribed(
				clientIdentifier,
				topic,
				geofence);

		// if already subscribed -> remove subscription id from now unrelated geofence parts
		if (subscribed != null) {
			topicAndGeofenceMapper.removeSubscriptionId(subscribed.left, topic, subscribed.right);
		}

		ImmutablePair<String, Integer> subscriptionId = clientDirectory.putSubscription(clientIdentifier,
				topic,
				geofence);

		if (subscriptionId == null) {
			logger.debug("Client {} is not connected", clientIdentifier);
			return new InternalServerMessage(clientIdentifier,
					ControlPacketType.SUBACK,
					new SUBACKPayload(ReasonCode.NotConnected));
		} else {
			topicAndGeofenceMapper.putSubscriptionId(subscriptionId, topic, geofence);
			logger.debug("Client {} subscribed to topic {} and geofence {}", clientIdentifier, topic, geofence);
			return new InternalServerMessage(clientIdentifier,
					ControlPacketType.SUBACK,
					new SUBACKPayload(ReasonCode.GrantedQoS0));
		}
	}

}
