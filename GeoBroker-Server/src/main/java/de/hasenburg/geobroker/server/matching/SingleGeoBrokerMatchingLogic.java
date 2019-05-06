package de.hasenburg.geobroker.server.matching;

import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.ReasonCode;
import de.hasenburg.geobroker.commons.model.message.payloads.*;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import de.hasenburg.geobroker.server.communication.InternalServerMessage;
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

	public SingleGeoBrokerMatchingLogic(ClientDirectory clientDirectory,
										TopicAndGeofenceMapper topicAndGeofenceMapper) {
		this.clientDirectory = clientDirectory;
		this.topicAndGeofenceMapper = topicAndGeofenceMapper;
	}

	@Override
	public void processCONNECT(InternalServerMessage message, Socket clients, Socket brokers) {
		CONNECTPayload payload = message.getPayload().getCONNECTPayload().get();

		InternalServerMessage response = CommonMatchingTasks.connectClientAtLocalBroker(message.getClientIdentifier(),
				payload.getLocation(),
				clientDirectory,
				logger);

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
		PINGREQPayload payload = message.getPayload().getPINGREQPayload().get();

		InternalServerMessage response =
				CommonMatchingTasks.updateClientLocationAtLocalBroker(message.getClientIdentifier(),
						payload.getLocation(),
						clientDirectory,
						logger);

		logger.trace("Sending response " + response);
		response.getZMsg().send(clients);
	}

	@Override
	public void processSUBSCRIBE(InternalServerMessage message, Socket clients, Socket brokers) {
		SUBSCRIBEPayload payload = message.getPayload().getSUBSCRIBEPayload().get();

		InternalServerMessage response = CommonMatchingTasks.subscribeAtLocalBroker(message.getClientIdentifier(),
				clientDirectory,
				topicAndGeofenceMapper,
				payload.getTopic(),
				payload.getGeofence(),
				logger);

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
			subscriptionIds.removeIf(subId -> !payload.getGeofence()
													  .contains(clientDirectory.getClientLocation(subId.left)));

			// publish message to remaining subscribers
			for (ImmutablePair<String, Integer> subscriptionId : subscriptionIds) {
				String subscriberClientIdentifier = subscriptionId.left;
				logger.debug("Client {} is a subscriber", subscriberClientIdentifier);
				InternalServerMessage toPublish = new InternalServerMessage(subscriberClientIdentifier,
						ControlPacketType.PUBLISH,
						payload);
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
		logger.warn("Unsupported operation, message is discarded");
	}
}
