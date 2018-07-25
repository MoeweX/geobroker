package de.hasenburg.geofencebroker.tasks;

import de.hasenburg.geofencebroker.communication.ControlPacketType;
import de.hasenburg.geofencebroker.communication.ReasonCode;
import de.hasenburg.geofencebroker.communication.RouterCommunicator;
import de.hasenburg.geofencebroker.model.RouterMessage;
import de.hasenburg.geofencebroker.model.connections.Connection;
import de.hasenburg.geofencebroker.model.connections.ConnectionManager;
import de.hasenburg.geofencebroker.model.connections.Subscription;
import de.hasenburg.geofencebroker.model.payload.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMsg;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A Task that continuously processes messages.
 * 
 * @author jonathanhasenburg
 *
 */
@SuppressWarnings("ConstantConditions")
class MessageProcessorTask extends Task<Boolean> {

	private static final Logger logger = LogManager.getLogger();

	BlockingQueue<ZMsg> messageQueue;
	RouterCommunicator routerCommunicator;
	ConnectionManager connectionManager;

	protected MessageProcessorTask(TaskManager taskManager, BlockingQueue<ZMsg> messageQueue, RouterCommunicator routerCommunicator, ConnectionManager connectionManager) {
		super(TaskManager.TaskName.MESSAGE_PROCESSOR_TASK, taskManager);
		this.messageQueue = messageQueue;
		this.routerCommunicator = routerCommunicator;
		this.connectionManager = connectionManager;
	}

	@Override
	public Boolean executeFunctionality() {

		int queuedMessages = 0;

		while (!Thread.currentThread().isInterrupted()) {
			try {

				if (queuedMessages != messageQueue.size()) {
					logger.trace("Number of queued messages: " + messageQueue.size());
					queuedMessages = messageQueue.size();
				}

				Optional<RouterMessage> messageO = RouterMessage.buildRouterMessage(messageQueue.poll(10, TimeUnit.SECONDS));
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
				}, () -> logger.debug("Did not receive a message for 10 seconds"));

			} catch (InterruptedException e) {
				logger.info("Task was interrupted, ending it.");
				Thread.currentThread().interrupt(); // when interrupted, the task will end which is fine
			}

		}

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
			response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.CONNACK, new CONNACKPayload());
			connectionManager.putConnection(connection);
		}

		routerCommunicator.sendRouterMessage(response);
	}

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

		routerCommunicator.sendRouterMessage(response);
	}

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

		routerCommunicator.sendRouterMessage(response);
	}

	public void processUNSUBSCRIBEforConnection(RouterMessage message) {
		throw new RuntimeException("Not yet implemented");
	}

	public void processPublish(RouterMessage message) {
		RouterMessage response;

		Connection connection = connectionManager.getConnection(message.getClientIdentifier());
		if (connection != null) {
			PUBLISHPayload payload = message.getPayload().getPUBLISHPayload().get();

			List<Connection> activeConnections = connectionManager.getActiveConnections();
			logger.debug("Publishing topic {} to all subscribers", payload.getTopic());

			// send message to every connection that has topic and whose location is published geofence
			for (Connection targetConnection : activeConnections) {
				if (targetConnection.shouldGetMessage(payload.getTopic(), payload.getGeofence())) {
					logger.trace("Client {} is a subscriber", targetConnection.getClientIdentifier());
					RouterMessage toPublish = new RouterMessage(targetConnection.getClientIdentifier(),
							ControlPacketType.PUBLISH, payload);
					routerCommunicator.sendRouterMessage(toPublish);
				}
			}

			response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.PUBACK,
					new PUBACKPayload(ReasonCode.Success));

		} else {
			response = new RouterMessage(message.getClientIdentifier(), ControlPacketType.PUBACK,
					new PUBACKPayload(ReasonCode.NotConnected));
		}

		routerCommunicator.sendRouterMessage(response);
	}
}
