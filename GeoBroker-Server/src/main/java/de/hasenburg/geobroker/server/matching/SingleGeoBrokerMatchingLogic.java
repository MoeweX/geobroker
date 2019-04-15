package de.hasenburg.geobroker.server.matching;

import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.ReasonCode;
import de.hasenburg.geobroker.commons.model.message.payloads.*;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import de.hasenburg.geobroker.server.communication.InternalServerMessage;
import de.hasenburg.geobroker.server.distribution.BrokerAreaManager;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import de.hasenburg.geobroker.server.storage.client.ClientDirectory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ.Socket;

import java.util.Set;

/**
 * One GeoBroker instance that does not communicate with others. Uses the {@link de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper}.
 */
@SuppressWarnings("OptionalGetWithoutIsPresent") // see IMatchingLogic for reasoning
public class SingleGeoBrokerMatchingLogic implements IMatchingLogic {

	private static final Logger logger = LogManager.getLogger();

	private final ClientDirectory clientDirectory;
	private final TopicAndGeofenceMapper topicAndGeofenceMapper;

	public SingleGeoBrokerMatchingLogic(ClientDirectory clientDirectory, TopicAndGeofenceMapper topicAndGeofenceMapper) {
		this.clientDirectory = clientDirectory;
		this.topicAndGeofenceMapper = topicAndGeofenceMapper;
	}

	@Override
	public void processCONNECT(InternalServerMessage message, Socket clients, Socket brokers) {
		InternalServerMessage response;
		CONNECTPayload payload = message.getPayload().getCONNECTPayload().get();

		boolean success = clientDirectory.addClient(message.getClientIdentifier(), payload.getLocation());

		if (success) {
			logger.debug("Created client {}, acknowledging.", message.getClientIdentifier());
			response = new InternalServerMessage(message.getClientIdentifier(),
												 ControlPacketType.CONNACK,
												 new CONNACKPayload(ReasonCode.Success));
		} else {
			logger.debug("Client {} already exists, so protocol error. Disconnecting.", message.getClientIdentifier());
			clientDirectory.removeClient(message.getClientIdentifier());
			response = new InternalServerMessage(message.getClientIdentifier(),
												 ControlPacketType.DISCONNECT,
												 new DISCONNECTPayload(ReasonCode.ProtocolError));
		}

		logger.trace("Sending response " + response);
		response.getZMsg().send(clients);
	}

	@Override
	public void processDISCONNECT(InternalServerMessage message, Socket clients, Socket brokers) {
		DISCONNECTPayload payload = message.getPayload().getDISCONNECTPayload().get();

		boolean success = clientDirectory.removeClient(message.getClientIdentifier());
		if (!success) {
			logger.trace("Client for {} did not exist", message.getClientIdentifier());
			return;
		}

		logger.debug("Disconnected client {}, code {}", message.getClientIdentifier(), payload.getReasonCode());

	}

	@Override
	public void processPINGREQ(InternalServerMessage message, Socket clients, Socket brokers) {
		InternalServerMessage response;
		PINGREQPayload payload = message.getPayload().getPINGREQPayload().get();

		boolean success = clientDirectory.updateClientLocation(message.getClientIdentifier(), payload.getLocation());
		if (success) {
			logger.debug("Updated location of {} to {}", message.getClientIdentifier(), payload.getLocation());

			response = new InternalServerMessage(message.getClientIdentifier(),
												 ControlPacketType.PINGRESP,
												 new PINGRESPPayload(ReasonCode.LocationUpdated));
		} else {
			logger.debug("Client {} is not connected", message.getClientIdentifier());
			response = new InternalServerMessage(message.getClientIdentifier(),
												 ControlPacketType.PINGRESP,
												 new PINGRESPPayload(ReasonCode.NotConnected));
		}

		logger.trace("Sending response " + response);
		response.getZMsg().send(clients);
	}

	@Override
	public void processSUBSCRIBE(InternalServerMessage message, Socket clients, Socket brokers) {
		InternalServerMessage response;
		SUBSCRIBEPayload payload = message.getPayload().getSUBSCRIBEPayload().get();

		ImmutablePair<ImmutablePair<String, Integer>, Geofence> subscribed =
				clientDirectory.checkIfSubscribed(message.getClientIdentifier(),
												  payload.getTopic(),
												  payload.getGeofence());

		// if already subscribed -> remove subscription id from now unrelated geofence parts
		if (subscribed != null) {
			topicAndGeofenceMapper.removeSubscriptionId(subscribed.left, payload.getTopic(), subscribed.right);
		}

		ImmutablePair<String, Integer> subscriptionId = clientDirectory.putSubscription(message.getClientIdentifier(),
																						payload.getTopic(),
																						payload.getGeofence());

		if (subscriptionId == null) {
			logger.debug("Client {} is not connected", message.getClientIdentifier());
			response = new InternalServerMessage(message.getClientIdentifier(),
												 ControlPacketType.SUBACK,
												 new SUBACKPayload(ReasonCode.NotConnected));
		} else {
			topicAndGeofenceMapper.putSubscriptionId(subscriptionId, payload.getTopic(), payload.getGeofence());
			logger.debug("Client {} subscribed to topic {} and geofence {}",
						 message.getClientIdentifier(),
						 payload.getTopic(),
						 payload.getGeofence());
			response = new InternalServerMessage(message.getClientIdentifier(),
												 ControlPacketType.SUBACK,
												 new SUBACKPayload(ReasonCode.GrantedQoS0));
		}

		logger.trace("Sending response " + response);
		response.getZMsg().send(clients);
	}

	@Override
	public void processUNSUBSCRIBE(InternalServerMessage message, Socket clients, Socket brokers) {
		// TODO Implement
		throw new RuntimeException("Not yet implemented");
	}

	@Override
	public void processPUBLISH(InternalServerMessage message, Socket clients, Socket brokers) {
		InternalServerMessage response;
		PUBLISHPayload payload = message.getPayload().getPUBLISHPayload().get();

		Location publisherLocation = clientDirectory.getClientLocation(message.getClientIdentifier());
		if (publisherLocation != null) {
			logger.debug("Publishing topic {} to all subscribers", payload.getTopic());

			// get subscriptions that have a geofence containing the publisher location
			Set<ImmutablePair<String, Integer>> subscriptionIds =
					topicAndGeofenceMapper.getSubscriptionIds(payload.getTopic(), publisherLocation);

			// only keep subscription if subscriber location is insider publisher geofence
			subscriptionIds.removeIf(subId -> !payload
					.getGeofence()
					.contains(clientDirectory.getClientLocation(subId.left)));

			// publish message to remaining subscribers
			for (ImmutablePair<String, Integer> subscriptionId : subscriptionIds) {
				String subscriberClientIdentifier = subscriptionId.left;
				logger.debug("Client {} is a subscriber", subscriberClientIdentifier);
				InternalServerMessage toPublish =
						new InternalServerMessage(subscriberClientIdentifier, ControlPacketType.PUBLISH, payload);
				logger.trace("Publishing " + toPublish);
				toPublish.getZMsg().send(clients);
			}

			if (subscriptionIds.isEmpty()) {
				logger.debug("No subscriber exists.");
				response = new InternalServerMessage(message.getClientIdentifier(),
													 ControlPacketType.PUBACK,
													 new PUBACKPayload(ReasonCode.NoMatchingSubscribers));
			} else {
				response = new InternalServerMessage(message.getClientIdentifier(),
													 ControlPacketType.PUBACK,
													 new PUBACKPayload(ReasonCode.Success));
			}

		} else {
			logger.debug("Client {} is not connected", message.getClientIdentifier());
			response = new InternalServerMessage(message.getClientIdentifier(),
												 ControlPacketType.PUBACK,
												 new PUBACKPayload(ReasonCode.NotConnected));
		}

		// send response to publisher
		logger.trace("Sending response " + response);
		response.getZMsg().send(clients);
	}

	@Override
	public void processBrokerForwardPublish(InternalServerMessage message, Socket clients, Socket brokers) {
		throw new RuntimeException("Unsupported Operation");
	}
}
