package de.hasenburg.geobroker.server.communication;

import de.hasenburg.geobroker.commons.communication.ZMQControlUtility;
import de.hasenburg.geobroker.commons.communication.ZMQProcess;
import de.hasenburg.geobroker.commons.model.BrokerInfo;
import de.hasenburg.geobroker.commons.model.message.ControlPacketType;
import de.hasenburg.geobroker.commons.model.message.ReasonCode;
import de.hasenburg.geobroker.commons.model.message.payloads.*;
import de.hasenburg.geobroker.commons.model.spatial.Geofence;
import de.hasenburg.geobroker.commons.model.spatial.Location;
import de.hasenburg.geobroker.server.distribution.BrokerAreaManager;
import de.hasenburg.geobroker.server.storage.TopicAndGeofenceMapper;
import de.hasenburg.geobroker.server.storage.client.ClientDirectory;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

class ZMQProcess_MessageProcessor extends ZMQProcess {

	private static final Logger logger = LogManager.getLogger();

	private final ClientDirectory clientDirectory;
	private final TopicAndGeofenceMapper topicAndGeofenceMapper;
	private final BrokerAreaManager brokerAreaManager;

	private int numberOfProcessedMessages = 0;

	// socket index
	private final int PROCESSOR_INDEX = 0;

	ZMQProcess_MessageProcessor(String identity, ClientDirectory clientDirectory,
								TopicAndGeofenceMapper topicAndGeofenceMapper, BrokerAreaManager brokerAreaManager) {
		super(identity);
		this.clientDirectory = clientDirectory;
		this.topicAndGeofenceMapper = topicAndGeofenceMapper;
		this.brokerAreaManager = brokerAreaManager;
	}

	@Override
	protected List<Socket> bindAndConnectSockets(ZContext context) {
		Socket[] socketArray = new Socket[1];

		Socket processor = context.createSocket(SocketType.DEALER);
		processor.setIdentity(identity.getBytes());
		processor.connect(ZMQProcess_Server.SERVER_INPROC_ADDRESS);
		socketArray[PROCESSOR_INDEX] = processor;

		return Arrays.asList(socketArray);
	}

	@Override
	protected void processZMQControlCommandOtherThanKill(ZMQControlUtility.ZMQControlCommand zmqControlCommand, ZMsg msg) {
		// no other commands are of interest
	}

	@Override
	protected void processZMsg(int socketIndex, ZMsg msg) {

		if (socketIndex != PROCESSOR_INDEX) {
			logger.error("Cannot process message for socket at index {}, as this index is not known.", socketIndex);
		}

		// start processing the message
		numberOfProcessedMessages++;

		Optional<InternalServerMessage> messageO = InternalServerMessage.buildMessage(msg);
		logger.trace("ZMQProcess_MessageProcessor {} processing message number {}", identity, numberOfProcessedMessages);

		if (messageO.isPresent()) {
			InternalServerMessage message = messageO.get();
			switch (message.getControlPacketType()) {
				case CONNECT:
					processCONNECT(message);
					break;
				case DISCONNECT:
					processDISCONNECT(message);
					break;
				case PINGREQ:
					processPINGREQ(message);
					break;
				case SUBSCRIBE:
					processSUBSCRIBEforConnection(message);
					break;
				case PUBLISH:
					processPublish(message);
					break;
				default:
					logger.warn("Cannot process message {}", message.toString());
			}
		} else {
			logger.warn("Received an incompatible message: {}", msg);
		}

	}

	@Override
	protected void shutdownCompleted() {
		logger.info("Shut down ZMQProcess_MessageProcessor {}", identity);
	}

	/*****************************************************************
	 * Message Processing
	 * 	- we already validated the messages above using #buildMessage()
	 * 	-> we expect the payload to be compatible with the control packet type
	 * 	-> we expect all fields to be set
	 ****************************************************************/

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	private void processCONNECT(InternalServerMessage message) {
		InternalServerMessage response;
		CONNECTPayload payload = message.getPayload().getCONNECTPayload().get();

		if (!handleResponsibility(message.getClientIdentifier(), payload.getLocation())) {
			return; // we are not responsible, client has been notified
		}

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
		response.getZMsg().send(sockets.get(PROCESSOR_INDEX));
	}

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	private void processDISCONNECT(InternalServerMessage message) {
		DISCONNECTPayload payload = message.getPayload().getDISCONNECTPayload().get();

		boolean success = clientDirectory.removeClient(message.getClientIdentifier());
		if (!success) {
			logger.trace("Client for {} did not exist", message.getClientIdentifier());
			return;
		}

		logger.debug("Disconnected client {}, code {}", message.getClientIdentifier(), payload.getReasonCode());
	}

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	private void processPINGREQ(InternalServerMessage message) {
		InternalServerMessage response;
		PINGREQPayload payload = message.getPayload().getPINGREQPayload().get();

		// check whether client has moved to another broker area
		if (!handleResponsibility(message.getClientIdentifier(), payload.getLocation())) {
			// TODO F: migrate client data to other broker, right now he has to update the information himself
			clientDirectory.removeClient(message.getClientIdentifier());
			return; // we are not responsible, client has been notified
		}

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
		response.getZMsg().send(sockets.get(PROCESSOR_INDEX));
	}

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	private void processSUBSCRIBEforConnection(InternalServerMessage message) {
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
		response.getZMsg().send(sockets.get(PROCESSOR_INDEX));
	}

	public void processUNSUBSCRIBEforConnection(InternalServerMessage message) {
		// TODO Implement
		throw new RuntimeException("Not yet implemented");
	}

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	private void processPublish(InternalServerMessage message) {
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
				toPublish.getZMsg().send(sockets.get(PROCESSOR_INDEX));
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
		response.getZMsg().send(sockets.get(PROCESSOR_INDEX));
	}

	/*****************************************************************
	 * Message Processing Helper
	 ****************************************************************/

	/**
	 * Checks whether this particular broker is responsible for the client with the given location.
	 * If not, sends a disconnect message with the responsible broker, if any exists.
	 * Otherwise, does nothing
	 *
	 * @return true, if this broker is responsible, otherwise false
	 */
	private boolean handleResponsibility(String clientIdentifier, Location clientLocation) {
		if (!brokerAreaManager.checkIfResponsibleForClientLocation(clientLocation)) {
			// get responsible broker
			BrokerInfo repBroker = brokerAreaManager.getOtherBrokerForClientLocation(clientLocation);

			InternalServerMessage response = new InternalServerMessage(clientIdentifier,
												 ControlPacketType.DISCONNECT,
												 new DISCONNECTPayload(ReasonCode.WrongBroker, repBroker));
			logger.debug("Not responsible for client {}, responsible broker is {}", clientIdentifier, repBroker);

			response.getZMsg().send(sockets.get(PROCESSOR_INDEX));
			return false;
		}
		return true;
	}

}
