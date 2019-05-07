package de.hasenburg.geobroker.server.matching;

import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.ReasonCode;
import de.hasenburg.geobroker.commons.model.message.Topic;
import de.hasenburg.geobroker.commons.model.message.payloads.*;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import de.hasenburg.geobroker.server.communication.InternalServerMessage;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import de.hasenburg.geobroker.server.storage.client.ClientDirectory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ.Socket;

import java.util.Set;

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

	/**
	 * @param publisherLocation - the location of the publisher
	 */
	static ReasonCode publishMessageToLocalClients(Location publisherLocation, PUBLISHPayload publishPayload,
												   ClientDirectory clientDirectory,
												   TopicAndGeofenceMapper topicAndGeofenceMapper, Socket clients,
												   Logger logger) {

		logger.debug("Publishing topic {} to all subscribers", publishPayload.getTopic());

		// get subscriptions that have a geofence containing the publisher location
		Set<ImmutablePair<String, Integer>> subscriptionIds =
				topicAndGeofenceMapper.getSubscriptionIds(publishPayload.getTopic(), publisherLocation);

		// only keep subscription if subscriber location is insider message geofence
		subscriptionIds.removeIf(subId -> !publishPayload.getGeofence()
														 .contains(clientDirectory.getClientLocation(subId.left)));

		// publish message to remaining subscribers
		for (ImmutablePair<String, Integer> subscriptionId : subscriptionIds) {
			String subscriberClientIdentifier = subscriptionId.left;
			logger.debug("Client {} is a subscriber", subscriberClientIdentifier);
			InternalServerMessage toPublish = new InternalServerMessage(subscriberClientIdentifier,
					ControlPacketType.PUBLISH,
					publishPayload);
			logger.trace("Publishing " + toPublish);
			toPublish.getZMsg().send(clients);
		}

		if (subscriptionIds.isEmpty()) {
			logger.debug("No subscriber exists.");
			return ReasonCode.NoMatchingSubscribers;
		} else {
			return ReasonCode.Success;
		}

	}

}
