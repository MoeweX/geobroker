package de.hasenburg.geofencebroker.tasks;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.ReasonCode;
import de.hasenburg.geofencebroker.model.RouterMessage;
import de.hasenburg.geofencebroker.model.connections.Connection;
import de.hasenburg.geofencebroker.model.connections.ConnectionManager;
import de.hasenburg.geofencebroker.model.connections.Subscription;
import de.hasenburg.geofencebroker.model.payload.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Optional;

/**
 * A Task that continuously processes messages.
 * 
 * @author jonathanhasenburg
 *
 */
class ZMQMessageProcessorTask extends Task<Boolean> {

	private static final Logger logger = LogManager.getLogger();

	ConnectionManager connectionManager;
	ZContext context;
	ZMQ.Socket processor;

	protected ZMQMessageProcessorTask(TaskManager taskManager, ZContext context, ConnectionManager connectionManager) {
		super(TaskManager.TaskName.ZMQ_MESSAGE_PROCESSOR_TASK, taskManager);
		this.connectionManager = connectionManager;
		this.context = context;
	}

	@Override
	public Boolean executeFunctionality() {

		processor = context.createSocket(ZMQ.DEALER);
		processor.connect("inproc://backend");

		while (!Thread.currentThread().isInterrupted()) {

			logger.debug("ZMQMessageProcessor waiting for message");
			ZMsg zMsg;

			try {
				zMsg = ZMsg.recvMsg(processor);
			} catch (ZMQException e) {
				if (e.getErrorCode() == ZMQ.Error.ETERM.getCode()) {
					break; // recvMsg was interrupted
				} else {
					logger.error("Exception while receiving, killing ZMQMessageProcessor", e);
					break;
				}
			}

			Optional<RouterMessage> messageO = RouterMessage.buildRouterMessage(zMsg);
			messageO.ifPresentOrElse(message -> {
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
						logger.debug("Cannot process message {}", message.toString());
				}
			}, () -> logger.warn("Received an incompatible message", zMsg));

		}

		context.destroySocket(processor);
		logger.debug("ZMQMessageProcessor socket destroyed.");
		logger.info("Shut down ZMQMessageProcessor");

		return true;
	}

	/*****************************************************************
	 * Message Processing
	 * 	- we already validated the messages above using #buildRouterMessage()
	 * 	-> we expect the payload to be compatible with the control packet type
	 * 	-> we expect all fields to be set
	 ****************************************************************/

	private void processCONNECT(RouterMessage message) {
		RouterMessage response;

		Connection connection = connectionManager.getConnection(message.getClientIdentifier());
		if (connection != null) {
			logger.debug("Connection already exists for {}, so protocol error. Disconnecting.", message.getClientIdentifier());
			connectionManager.removeConnection(message.getClientIdentifier());
			response = new RouterMessage(
					message.getClientIdentifier(), ControlPacketType.DISCONNECT,
					new DISCONNECTPayload(ReasonCode.ProtocolError));
		} else {
			connection = new Connection(message.getClientIdentifier());
			logger.debug("Created connection for client {}, acknowledging.", message.getClientIdentifier());
			response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.CONNACK, new CONNACKPayload(ReasonCode.Success));
			connectionManager.putConnection(connection);
		}

		response.getZMsg().send(processor);
	}

	@SuppressWarnings("ConstantConditions")
	private void processDISCONNECT(RouterMessage message) {
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
	private void processPINGREQ(RouterMessage message) {
		RouterMessage response;

		Connection connection = connectionManager.getConnection(message.getClientIdentifier());
		if (connection != null) {
			PINGREQPayload payload = message.getPayload().getPINGREQPayload().get();

			// update location
			connection.updateLocation(payload.getLocation());
			logger.debug("Updated location of {} to {}", message.getClientIdentifier(), payload.getLocation());

			response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.PINGRESP, new PINGRESPPayload(ReasonCode.LocationUpdated));
		} else {
			logger.trace("Client {} is not connected", message.getClientIdentifier());
			response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.PINGRESP, new PINGRESPPayload(ReasonCode.NotConnected));
		}

		response.getZMsg().send(processor);
	}

	@SuppressWarnings("ConstantConditions")
	private void processSUBSCRIBEforConnection(RouterMessage message) {
		RouterMessage response;

		Connection connection = connectionManager.getConnection(message.getClientIdentifier());
		if (connection != null) {
			SUBSCRIBEPayload payload = message.getPayload().getSUBSCRIBEPayload().get();

			// update subscription
			Subscription subscription = new Subscription(payload.getTopic(), payload.getGeofence());
			connection.putSubscription(subscription);
			logger.debug("Client {} subscribed to topic {}", message.getClientIdentifier(), payload.getTopic());
			response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.SUBACK,
					new SUBACKPayload(ReasonCode.GrantedQoS1));
		} else {
			response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.SUBACK,
					new SUBACKPayload(ReasonCode.NotConnected));
		}

		response.getZMsg().send(processor);
	}

	public void processUNSUBSCRIBEforConnection(RouterMessage message) {
		throw new RuntimeException("Not yet implemented");
	}

	@SuppressWarnings("ConstantConditions")
	public void processPublish(RouterMessage message) {
		RouterMessage response;

		Connection connection = connectionManager.getConnection(message.getClientIdentifier());
		if (connection != null) {
			PUBLISHPayload payload = message.getPayload().getPUBLISHPayload().get();

			logger.debug("Publishing topic {} to all subscribers", payload.getTopic());
			List<Connection> subscribers = connectionManager
					.getSubscribers(payload.getTopic(), payload.getGeofence(), connection.getLocation().orElseGet(null));

			for (Connection subscriber : subscribers) {
				logger.trace("Client {} is a subscriber", subscriber.getClientIdentifier());
				RouterMessage toPublish = new RouterMessage(subscriber.getClientIdentifier(),
						ControlPacketType.PUBLISH, payload);
				toPublish.getZMsg().send(processor);
			}

			if (!subscribers.isEmpty()) {
				response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.PUBACK,
						new PUBACKPayload(ReasonCode.Success));
			} else {
				logger.trace("No subscriber exists.");
				response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.PUBACK,
						new PUBACKPayload(ReasonCode.NoMatchingSubscribers));
			}

		} else {
			response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.PUBACK,
					new PUBACKPayload(ReasonCode.NotConnected));
		}

		// send response to publisher
		response.getZMsg().send(processor);
	}
}
