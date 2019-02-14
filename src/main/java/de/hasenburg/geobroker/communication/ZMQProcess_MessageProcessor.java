package de.hasenburg.geobroker.communication;

import de.hasenburg.geobroker.main.BenchmarkHelper;
import de.hasenburg.geobroker.model.InternalBrokerMessage;
import de.hasenburg.geobroker.model.clients.ClientDirectory;
import de.hasenburg.geobroker.model.payload.*;
import de.hasenburg.geobroker.model.spatial.Geofence;
import de.hasenburg.geobroker.model.spatial.Location;
import de.hasenburg.geobroker.model.storage.TopicAndGeofenceMapper;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.Optional;
import java.util.Set;

class ZMQProcess_MessageProcessor implements Runnable {

	private static final Logger logger = LogManager.getLogger();
	private static final int TIMEOUT_SECONDS = 10; // logs when not received in time, but repeats

	ClientDirectory clientDirectory;
	TopicAndGeofenceMapper topicAndGeofenceMapper;
	String identity;
	ZContext context;
	ZMQ.Socket processor;

	protected ZMQProcess_MessageProcessor(String identity, ZContext context, ClientDirectory clientDirectory,
										  TopicAndGeofenceMapper topicAndGeofenceMapper) {
		this.identity = identity;
		this.clientDirectory = clientDirectory;
		this.context = context;
		this.topicAndGeofenceMapper = topicAndGeofenceMapper;
	}

	@Override
	public void run() {
		Thread.currentThread().setName(identity);

		processor = context.createSocket(SocketType.DEALER);
		processor.setIdentity(identity.getBytes());
		processor.connect(ZMQProcess_Broker.BROKER_PROCESSING_BACKEND);

		ZMQ.Poller poller = context.createPoller(1);
		int zmqControlIndex = ZMQControlUtility.connectWithPoller(context, poller, identity);
		poller.register(processor, ZMQ.Poller.POLLIN);

		int number = 1;

		while (!Thread.currentThread().isInterrupted()) {

			logger.trace("ZMQProcess_MessageProcessor {} waiting {}s for a message",
						 new String(processor.getIdentity()),
						 TIMEOUT_SECONDS);
			poller.poll(TIMEOUT_SECONDS * 1000);

			if (poller.pollin(zmqControlIndex)) {
				if (ZMQControlUtility
						.getCommand(poller, zmqControlIndex)
						.equals(ZMQControlUtility.ZMQControlCommand.KILL)) {
					break;
				}
			} else if (poller.pollin(1)) {
				ZMsg zMsg = ZMsg.recvMsg(processor);
				number++;
				Optional<InternalBrokerMessage> messageO = InternalBrokerMessage.buildMessage(zMsg);
				logger.debug("ZMQProcess_MessageProcessor {} processing message number {}",
							 new String(processor.getIdentity()),
							 number);
				if (messageO.isPresent()) {
					InternalBrokerMessage message = messageO.get();
					long time = System.nanoTime();
					switch (message.getControlPacketType()) {
						case CONNECT:
							processCONNECT(message);
							BenchmarkHelper.addEntry("processCONNECT", System.nanoTime() - time);
							break;
						case DISCONNECT:
							processDISCONNECT(message);
							BenchmarkHelper.addEntry("processDISCONNECT", System.nanoTime() - time);
							break;
						case PINGREQ:
							processPINGREQ(message);
							BenchmarkHelper.addEntry("processPINGREQ", System.nanoTime() - time);
							break;
						case SUBSCRIBE:
							processSUBSCRIBEforConnection(message);
							BenchmarkHelper.addEntry("processSUBSCRIBEforConnection", System.nanoTime() - time);
							break;
						case PUBLISH:
							processPublish(message);
							BenchmarkHelper.addEntry("processPublish", System.nanoTime() - time);
							break;
						default:
							logger.warn("Cannot process message {}", message.toString());
					}
				} else {
					logger.warn("Received an incompatible message", zMsg);
				}
			} else {
				logger.debug("Did not receive a message for {}s", TIMEOUT_SECONDS);
			}

		} // end while loop

		// sub control socket (might be optional, kill nevertheless)
		context.destroySocket(poller.getSocket(0));

		// processor socket
		context.destroySocket(processor);
		logger.info("Shut down ZMQProcess_MessageProcessor, socket were destroyed.");
	}

	/*****************************************************************
	 * Message Processing
	 * 	- we already validated the messages above using #buildMessage()
	 * 	-> we expect the payload to be compatible with the control packet type
	 * 	-> we expect all fields to be set
	 ****************************************************************/

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	private void processCONNECT(InternalBrokerMessage message) {
		InternalBrokerMessage response;
		CONNECTPayload payload = message.getPayload().getCONNECTPayload().get();

		boolean success = clientDirectory.addClient(message.getClientIdentifier(), payload.getLocation());

		if (success) {
			logger.debug("Created client for client {}, acknowledging.", message.getClientIdentifier());
			response = new InternalBrokerMessage(message.getClientIdentifier(),
												 ControlPacketType.CONNACK,
												 new CONNACKPayload(ReasonCode.Success));
		} else {
			logger.debug("Client already exists for {}, so protocol error. Disconnecting.",
						 message.getClientIdentifier());
			clientDirectory.removeClient(message.getClientIdentifier());
			response = new InternalBrokerMessage(message.getClientIdentifier(),
												 ControlPacketType.DISCONNECT,
												 new DISCONNECTPayload(ReasonCode.ProtocolError));
		}

		logger.trace("Sending response " + response);
		response.getZMsg().send(processor);
	}

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	private void processDISCONNECT(InternalBrokerMessage message) {
		DISCONNECTPayload payload = message.getPayload().getDISCONNECTPayload().get();

		boolean success = clientDirectory.removeClient(message.getClientIdentifier());
		if (!success) {
			logger.trace("Client for {} did not exist", message.getClientIdentifier());
			return;
		}

		logger.debug("Disconnected client {}, code {}", message.getClientIdentifier(), payload.getReasonCode());
	}

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	private void processPINGREQ(InternalBrokerMessage message) {
		InternalBrokerMessage response;
		PINGREQPayload payload = message.getPayload().getPINGREQPayload().get();

		boolean success = clientDirectory.updateClientLocation(message.getClientIdentifier(), payload.getLocation());
		if (success) {
			logger.debug("Updated location of {} to {}", message.getClientIdentifier(), payload.getLocation());

			response = new InternalBrokerMessage(message.getClientIdentifier(),
												 ControlPacketType.PINGRESP,
												 new PINGRESPPayload(ReasonCode.LocationUpdated));
		} else {
			logger.debug("Client {} is not connected", message.getClientIdentifier());
			response = new InternalBrokerMessage(message.getClientIdentifier(),
												 ControlPacketType.PINGRESP,
												 new PINGRESPPayload(ReasonCode.NotConnected));
		}

		logger.trace("Sending response " + response);
		response.getZMsg().send(processor);
	}

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	private void processSUBSCRIBEforConnection(InternalBrokerMessage message) {
		InternalBrokerMessage response;
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
			response = new InternalBrokerMessage(message.getClientIdentifier(),
												 ControlPacketType.SUBACK,
												 new SUBACKPayload(ReasonCode.NotConnected));
		} else {
			topicAndGeofenceMapper.putSubscriptionId(subscriptionId, payload.getTopic(), payload.getGeofence());
			logger.debug("Client {} subscribed to topic {} and geofence {}",
						 message.getClientIdentifier(),
						 payload.getTopic(),
						 payload.getGeofence());
			response = new InternalBrokerMessage(message.getClientIdentifier(),
												 ControlPacketType.SUBACK,
												 new SUBACKPayload(ReasonCode.GrantedQoS0));
		}

		logger.trace("Sending response " + response);
		response.getZMsg().send(processor);
	}

	public void processUNSUBSCRIBEforConnection(InternalBrokerMessage message) {
		// TODO Implement
		throw new RuntimeException("Not yet implemented");
	}

	@SuppressWarnings("OptionalGetWithoutIsPresent")
	public void processPublish(InternalBrokerMessage message) {
		InternalBrokerMessage response;
		PUBLISHPayload payload = message.getPayload().getPUBLISHPayload().get();

		Location publisherLocation = clientDirectory.getClientLocation(message.getClientIdentifier());
		if (publisherLocation != null) {
			logger.debug("Publishing topic {} to all subscribers", payload.getTopic());

			// get subscriptions that have a geofence containing the publisher location
			Set<ImmutablePair<String, Integer>> subscriptionIds =
					topicAndGeofenceMapper.getSubscriptionIds(payload.getTopic(), publisherLocation);

			//			// only keep subscription if subscriber location is insider publisher geofence
			//			subscriptionIds.removeIf(subId -> !payload
			//					.getGeofence()
			//					.contains(clientDirectory.getClientLocation(subId.left)));

			// publish message to remaining subscribers
			for (ImmutablePair<String, Integer> subscriptionId : subscriptionIds) {
				String subscriberClientIdentifier = subscriptionId.left;
				logger.debug("Client {} is a subscriber", subscriberClientIdentifier);
				InternalBrokerMessage toPublish =
						new InternalBrokerMessage(subscriberClientIdentifier, ControlPacketType.PUBLISH, payload);
				logger.trace("Publishing " + toPublish);
				toPublish.getZMsg().send(processor);
			}

			if (subscriptionIds.isEmpty()) {
				logger.debug("No subscriber exists.");
				response = new InternalBrokerMessage(message.getClientIdentifier(),
													 ControlPacketType.PUBACK,
													 new PUBACKPayload(ReasonCode.NoMatchingSubscribers));
			} else {
				response = new InternalBrokerMessage(message.getClientIdentifier(),
													 ControlPacketType.PUBACK,
													 new PUBACKPayload(ReasonCode.Success));
			}

		} else {
			logger.debug("Client {} is not connected", message.getClientIdentifier());
			response = new InternalBrokerMessage(message.getClientIdentifier(),
												 ControlPacketType.PUBACK,
												 new PUBACKPayload(ReasonCode.NotConnected));
		}

		// send response to publisher
		logger.trace("Sending response " + response);
		response.getZMsg().send(processor);
	}

}
