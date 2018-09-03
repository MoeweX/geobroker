package de.hasenburg.geofencebroker.communication;

import de.hasenburg.geofencebroker.main.BenchmarkHelper;
import de.hasenburg.geofencebroker.model.InternalBrokerMessage;
import de.hasenburg.geofencebroker.model.connections.Connection;
import de.hasenburg.geofencebroker.model.connections.ConnectionManager;
import de.hasenburg.geofencebroker.model.connections.Subscription;
import de.hasenburg.geofencebroker.model.payload.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

import java.util.List;
import java.util.Optional;

class ZMQProcess_MessageProcessor implements Runnable {

	private static final Logger logger = LogManager.getLogger();
	private static final int TIMEOUT_SECONDS = 10; // logs when not received in time, but repeats

	ConnectionManager connectionManager;
	String identity;
	ZContext context;
	ZMQ.Socket processor;

	protected ZMQProcess_MessageProcessor(String identity, ZContext context, ConnectionManager connectionManager) {
		this.identity = identity;
		this.connectionManager = connectionManager;
		this.context = context;
	}

	@Override
	public void run() {
		processor = context.createSocket(ZMQ.DEALER);
		processor.setIdentity(identity.getBytes());
		processor.connect(ZMQProcess_Broker.BROKER_PROCESSING_BACKEND);

		ZMQ.Poller poller = context.createPoller(1);
		int zmqControlIndex = ZMQControlUtility.connectWithPoller(context, poller, identity);
		poller.register(processor, ZMQ.Poller.POLLIN);

		int number = 1;

		while (!Thread.currentThread().isInterrupted()) {

			logger.trace("ZMQProcess_MessageProcessor {} waiting {}s for a message", new String(processor.getIdentity()), TIMEOUT_SECONDS);
			poller.poll(TIMEOUT_SECONDS * 1000);

			if (poller.pollin(zmqControlIndex)) {
				if (ZMQControlUtility.getCommand(poller, zmqControlIndex).equals(ZMQControlUtility.ZMQControlCommand.KILL)) {
					break;
				}
			} else if (poller.pollin(1)) {
				ZMsg zMsg = ZMsg.recvMsg(processor);
				number++;
				Optional<InternalBrokerMessage> messageO = InternalBrokerMessage.buildMessage(zMsg);
				logger.debug("ZMQProcess_MessageProcessor {} processing message number {}", new String(processor.getIdentity()), number);
				messageO.ifPresentOrElse(message -> {
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
				}, () -> logger.warn("Received an incompatible message", zMsg));
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

	private void processCONNECT(InternalBrokerMessage message) {
		InternalBrokerMessage response;

		Connection connection = connectionManager.getConnection(message.getClientIdentifier());
		if (connection != null) {
			logger.debug("Connection already exists for {}, so protocol error. Disconnecting.",
					message.getClientIdentifier());
			connectionManager.removeConnection(message.getClientIdentifier());
			response = new InternalBrokerMessage(
					message.getClientIdentifier(), ControlPacketType.DISCONNECT,
					new DISCONNECTPayload(ReasonCode.ProtocolError));
		} else {
			connection = new Connection(message.getClientIdentifier());
			logger.debug("Created connection for client {}, acknowledging.", message.getClientIdentifier());
			response = new InternalBrokerMessage(message.getClientIdentifier(), ControlPacketType.CONNACK,
					new CONNACKPayload(ReasonCode.Success));
			connectionManager.putConnection(connection);
		}

		logger.debug("Sending response " + response);
		response.getZMsg().send(processor);
	}

	@SuppressWarnings("ConstantConditions")
	private void processDISCONNECT(InternalBrokerMessage message) {
		Connection connection = connectionManager.getConnection(message.getClientIdentifier());
		if (connection == null) {
			logger.trace("Connection for {} did not exist", message.getClientIdentifier());
			return;
		}

		// remove connection
		DISCONNECTPayload payload = message.getPayload().getDISCONNECTPayload().get();
		logger.debug("Disconnected client {}, code {}", message.getClientIdentifier(), payload.getReasonCode());
		connectionManager.removeConnection(connection.getClientIdentifier());
	}

	@SuppressWarnings("ConstantConditions")
	private void processPINGREQ(InternalBrokerMessage message) {
		InternalBrokerMessage response;

		Connection connection = connectionManager.getConnection(message.getClientIdentifier());
		if (connection != null) {
			PINGREQPayload payload = message.getPayload().getPINGREQPayload().get();

			// update location
			connection.updateLocation(payload.getLocation());
			logger.debug("Updated location of {} to {}", message.getClientIdentifier(), payload.getLocation());

			response = new InternalBrokerMessage(message.getClientIdentifier(), ControlPacketType.PINGRESP,
					new PINGRESPPayload(ReasonCode.LocationUpdated));
		} else {
			logger.trace("Client {} is not connected", message.getClientIdentifier());
			response = new InternalBrokerMessage(message.getClientIdentifier(), ControlPacketType.PINGRESP,
					new PINGRESPPayload(ReasonCode.NotConnected));
		}

		logger.debug("Sending response " + response);
		response.getZMsg().send(processor);
	}

	@SuppressWarnings("ConstantConditions")
	private void processSUBSCRIBEforConnection(InternalBrokerMessage message) {
		InternalBrokerMessage response;

		Connection connection = connectionManager.getConnection(message.getClientIdentifier());
		if (connection != null) {
			SUBSCRIBEPayload payload = message.getPayload().getSUBSCRIBEPayload().get();

			// update subscription
			Subscription subscription = new Subscription(payload.getTopic(), payload.getGeofence());
			connection.putSubscription(subscription);
			logger.debug("Client {} subscribed to topic {}", message.getClientIdentifier(), payload.getTopic());
			response = new InternalBrokerMessage(message.getClientIdentifier(), ControlPacketType.SUBACK,
					new SUBACKPayload(ReasonCode.GrantedQoS1));
		} else {
			response = new InternalBrokerMessage(message.getClientIdentifier(), ControlPacketType.SUBACK,
					new SUBACKPayload(ReasonCode.NotConnected));
		}

		logger.debug("Sending response " + response);
		response.getZMsg().send(processor);
	}

	public void processUNSUBSCRIBEforConnection(InternalBrokerMessage message) {
		throw new RuntimeException("Not yet implemented");
	}

	@SuppressWarnings("ConstantConditions")
	public void processPublish(InternalBrokerMessage message) {
		InternalBrokerMessage response;

		Connection connection = connectionManager.getConnection(message.getClientIdentifier());
		if (connection != null) {
			PUBLISHPayload payload = message.getPayload().getPUBLISHPayload().get();

			logger.debug("Publishing topic {} to all subscribers", payload.getTopic());
			List<Connection> subscribers = connectionManager
					.getSubscribers(payload.getTopic(), payload.getGeofence(),
							connection.getLocation().orElseGet(null));

			for (Connection subscriber : subscribers) {
				logger.trace("Client {} is a subscriber", subscriber.getClientIdentifier());
				InternalBrokerMessage toPublish = new InternalBrokerMessage(subscriber.getClientIdentifier(),
						ControlPacketType.PUBLISH, payload);
				logger.debug("Publishing " + toPublish);
				toPublish.getZMsg().send(processor);
			}

			if (!subscribers.isEmpty()) {
				response = new InternalBrokerMessage(message.getClientIdentifier(), ControlPacketType.PUBACK,
						new PUBACKPayload(ReasonCode.Success));
			} else {
				logger.trace("No subscriber exists.");
				response = new InternalBrokerMessage(message.getClientIdentifier(), ControlPacketType.PUBACK,
						new PUBACKPayload(ReasonCode.NoMatchingSubscribers));
			}

		} else {
			response = new InternalBrokerMessage(message.getClientIdentifier(), ControlPacketType.PUBACK,
					new PUBACKPayload(ReasonCode.NotConnected));
		}

		// send response to publisher
		logger.debug("Sending response " + response);
		response.getZMsg().send(processor);
	}

}
